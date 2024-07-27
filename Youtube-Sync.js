import { Client } from "@notionhq/client";
import Bottleneck from "bottleneck";
import retry from "async-retry";
import youtubeDataApi from "@pipedream/youtube_data_api";

export default {
	name: "YouTube to Notion – View Counts",
	description:
		"Fetches view, like, and comment counts for each YouTube video in a Notion database. Uses the public YouTube Data API.",
	key: "youtube-notion-sync-views",
	version: "0.2.9",
	type: "action",
	props: {
		notion: {
			type: "app",
			app: "notion",
		},
		youtubeDataApi,
		databaseID: {
			type: "string",
			label: "Content Database",
			description:
				"Select the database that stores pages relating to your YouTube videos.",
			async options({ query, prevContext }) {
				if (this.notion) {
					try {
						const notion = new Client({
							auth: this.notion.$auth.oauth_access_token,
						});

						let start_cursor = prevContext?.cursor;

						const response = await notion.search({
							...(query ? { query } : {}),
							...(start_cursor ? { start_cursor } : {}),
							page_size: 50,
							filter: {
								value: "database",
								property: "object",
							},
							sorts: [
								{
									direction: "descending",
									property: "last_edited_time",
								},
							],
						});

						let allTasksDbs = response.results.filter((db) =>
							db.title?.[0]?.plain_text.includes("Master Content Tracker")
						);
						let nonTaskDbs = response.results.filter(
							(db) =>
								!db.title?.[0]?.plain_text.includes("Master Content Tracker")
						);
						let sortedDbs = [...allTasksDbs, ...nonTaskDbs];
						const UTregex = /Master Content Tracker/;
						const UTLabel = " – (used for Creator's Companion)";
						const UBregex = /Master Content Tracker \[\w*\]/;
						const UBLabel = " – (used for Creator's Companion)";
						const options = sortedDbs.map((db) => ({
							label: UBregex.test(db.title?.[0]?.plain_text)
								? db.title?.[0]?.plain_text + UBLabel
								: UTregex.test(db.title?.[0]?.plain_text)
								? db.title?.[0]?.plain_text + UTLabel
								: db.title?.[0]?.plain_text,
							value: db.id,
						}));

						return {
							context: {
								cursor: response.next_cursor,
							},
							options,
						};
					} catch (error) {
						console.error(error);
						return {
							context: {
								cursor: null,
							},
							options: [],
						};
					}
				} else {
					return {
						options: ["Please connect your Notion account first."],
					};
				}
			},
			reloadProps: true,
		},
	},
	async additionalProps() {
		if (!this.databaseID) return {};

		const notion = new Client({
			auth: this.notion.$auth.oauth_access_token,
		});

		const database = await notion.databases.retrieve({
			database_id: this.databaseID,
		});

		const properties = database.properties;

		const titleProps = Object.keys(properties).filter(
			(k) =>
				properties[k].type === "title" || properties[k].type === "rich_text"
		);

		const numberProps = Object.keys(properties).filter(
			(k) => properties[k].type === "number"
		);

		const dateProps = Object.keys(properties).filter(
			(k) => properties[k].type === "date"
		);

		const urlProps = Object.keys(properties).filter(
			(k) => properties[k].type === "url"
		);

		const relationProps = Object.keys(properties).filter(
			(k) => properties[k].type === "relation"
		);

		const props = {
			videoURL: {
				type: "string",
				label: "URL",
				description: "Select your Video URL property.",
				options: urlProps.map((prop) => ({ label: prop, value: prop })),
				optional: false,
			},
			viewCount: {
				type: "string",
				label: "View Count",
				description: "Select your View Count property.",
				options: numberProps.map((prop) => ({ label: prop, value: prop })),
				optional: false,
			},
			likeCount: {
				type: "string",
				label: "Like Count",
				description: "Select your Like Count property.",
				options: numberProps.map((prop) => ({ label: prop, value: prop })),
				optional: true,
			},
			commentCount: {
				type: "string",
				label: "Comment Count",
				description: "Select your Comment Count property.",
				options: numberProps.map((prop) => ({ label: prop, value: prop })),
				optional: true,
			},
			publishDate: {
				type: "string",
				label: "Publish Date",
				description:
					"If you want to automatically update your publish dates so they are accurate, select your Publish Date property. Otherwise, leave it blank.",
				options: dateProps.map((prop) => ({ label: prop, value: prop })),
				optional: true,
			},
			setThumbnail: {
				type: "boolean",
				label: "Set Thumbnail?",
				description:
					"If set to True, this script will automatically set your page cover to your video's public thumbnail.",
				default: false,
				optional: true,
			},
			videoTitle: {
				type: "string",
				label: "Video Title (Required)",
				description:
					"Select the title property for your videos. This propety supports both Title and Rich Text types, which means you can choose to update your actual page title in Notion, or use a separate Rich Text property to store your live video titles from YouTube.",
				options: titleProps.map((prop) => ({
					label: prop,
					value: JSON.stringify({
						name: prop,
						id: properties[prop].id,
						type: properties[prop].type,
					}),
				})),
				optional: true,
				reloadProps: true,
			},
			...(this.videoTitle && {
				updateTitle: {
					type: "boolean",
					label: "Update Title?",
					description:
						"If set to True, this script will automatically update your chosen Video Title property to the video's title.",
					default: false,
					optional: true,
				},
			}),
			disableRateBursting: {
				type: "boolean",
				label: "Disable Rate Bursting (Advanced",
				description:
					"If set to True, this script will disable API rate bursting for the Notion API, and will make no more than 3 requests per second. Set this to True if you are experiencing rate limiting issues.",
				default: false,
				optional: true,
			},
		};

		return props;
	},
	methods: {
		/**
		 * Fetches pages from the chosen Notion database, filtering for pages with YouTube URLs.
		 *
		 * @param {import('@notionhq/client').Client} notion
		 * @returns {Promise<Array<Object>>} - An array of objects, each containing the full page object response from the Notion API.
		 * @throws {Error} - Throws an error if the query fails.
		 *
		 * @example
		 * await this.fetchVidsFromNotion(notion)
		 * // Returns an array of page objects, each with the structure shown in the 200 response example here: https://developers.notion.com/reference/retrieve-a-page
		 */
		async fetchVidsFromNotion(notion) {
			// Pagination variables
			/** @type {boolean|undefined} */
			let hasMore;
			/** @type {string|undefined} */
			let token;

			// Set up our Bottleneck limiter
			const limiter = new Bottleneck({
				minTime: this.disableRateBursting === true ? 333 : 50,
				maxConcurrent: 1,
			});

			/**
			 * Handles rate limit errors by retrying after the specified wait time defined in the error headers.
			 *
			 * @param {Error} error - The error object returned by the API.
			 * @returns {number|undefined} - The wait time in milliseconds if the error is a rate limit error, otherwise undefined.
			 */
			const handleRateLimitError = (error) => {
				if (error.statusCode === 429) {
					console.log(
						`Job ${jobInfo.options.id} failed due to rate limit: ${error}`
					);
					const waitTime = error.headers["retry-after"]
						? parseInt(error.headers["retry-after"], 10)
						: 0.4;
					console.log(`Retrying after ${waitTime} seconds...`);
					return waitTime * 1000;
				}

				console.log(`Job ${jobInfo.options.id} failed: ${error}`);
				return;
			};

			limiter.on("error", handleRateLimitError);

			// Initial array for arrays of User or Project objects
			/** @type {Array<Object>} */
			const rows = [];

			// Query the Notion API until hasMore == false. Add all results to the rows array
			while (hasMore == undefined || hasMore == true) {
				try {
					await retry(
						async (bail) => {
							let resp;

							let params = {
								page_size: 100,
								start_cursor: token,
							};

							try {
								params = {
									...params,
									database_id: this.databaseID,
									filter: {
										or: [
											{
												property: this.videoURL,
												url: {
													contains: "youtube.com",
												},
											},
											{
												property: this.videoURL,
												url: {
													contains: "youtu.be",
												},
											},
										],
									},
								};
								resp = await limiter.schedule(() =>
									notion.databases.query(params)
								);
								rows.push(...resp.results);

								hasMore = resp.has_more;
								if (resp.next_cursor) {
									token = resp.next_cursor;
								}
							} catch (error) {
								if (400 <= error.status && error.status <= 409) {
									// Don't retry for errors 400-409
									bail(error);
									return;
								}

								if (
									error.status === 500 ||
									error.status === 503 ||
									error.status === 504
								) {
									// Retry on 500, 503, and 504
									throw error;
								}

								// Don't retry for other errors
								bail(error);
							}
						},
						{
							retries: 2,
							onRetry: (error, attempt) => {
								console.log(`Attempt ${attempt} failed. Retrying...`);
							},
						}
					);
				} catch (err) {
					throw new Error(
						`Error querying Notion to fetch videos. Full error details:\n\n${err}`
					);
				}
			}

			return rows;
		},
		/**
		 * Extracts the video ID from the provided YouTube URL.
		 * Supports both standard YouTube URLs and YouTube Shorts URLs.
		 *
		 * @param {string} url - The YouTube URL to extract the video ID from.
		 * @returns {string|null} - The video ID extracted from the URL, or null if no ID is found.
		 *
		 * @example
		 * this.getVideoId("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
		 * // Returns "dQw4w9WgXcQ"
		 *
		 * @example
		 * this.getVideoId("https://www.youtube.com/shorts/kV626LjZ2xs")
		 * // Returns "kV626LjZ2xs"
		 */
		getVideoId(url) {
			let videoIdRegex;
			if (url.includes("shorts")) {
				videoIdRegex = /shorts\/(.{11})/;
			} else {
				videoIdRegex =
					/(?:youtube(?:-nocookie)?\.com\/(?:[^/\n\s]+\/\S+\/|(?:v|vi|e(?:mbed)?)\/|\S*?[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
			}
			const match = url.match(videoIdRegex);
			if (!match) {
				console.warn(`No video ID found for URL: ${url}`);
			}
			return match ? match[1] : null;
		},
		/**
		 * Creates a multi-dimensional array of video objects from the provided array of Notion pages.
		 * Each sub-array contains up to 50 video objects in order to comply with the YouTube Data API's limits.
		 *
		 * @param {Array<Object>} rows - An array of Notion page objects, each containing the full page object response from the Notion API.
		 * @returns {Array<Array<Object>>} - A multi-dimensional array of video objects, each containing the video ID and URL.
		 *
		 * @example
		 * this.buildVideoArray(rows)
		 * // [ [ { id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', videoId: 'uvkmvXbEfec', url: 'https://www.youtube.com/watch?v=uvkmvXbEfec' } ] ]
		 */
		buildVideoArray(rows) {
			if (!Array.isArray(rows)) {
				throw new Error("The provided rows must be an array.");
			}

			/** Construct an array of simplified objects with the Notion Page ID, YouTube Video ID, and URL.
			 * Filter out any rows that do not have a valid video URL.
			 * */
			const validVideoArray = rows
				.map((row) => {
					if (!row.properties || !row.properties[this.videoURL]) {
						console.warn(`Row ${row.id} does not have a valid video URL.`);
						return null;
					}
					const url = row.properties[this.videoURL].url;
					const videoId = this.getVideoId(url);
					return videoId ? { id: row.id, videoId, url } : null;
				})
				.filter(Boolean);

			// Split the returned array into chunks of 50 videos
			const chunkedVideoArray = [];
			for (let i = 0; i < validVideoArray.length; i += 50) {
				chunkedVideoArray.push(validVideoArray.slice(i, i + 50));
			}

			return chunkedVideoArray;
		},
		/**
		 * Fetches video data from YouTube Data API for the provided video IDs.
		 *
		 * @param {Array<Array<Object>>} chunkedRows - A multi-dimensional array of video objects, each containing the video ID and URL. Each inner array can contain up to 50 video objects.
		 * @returns {Promise<Array<Object>>} An array of video objects from the YouTube Data API, each containing the video's statistics and snippet data.
		 * @throws {Error} If there's an unrecoverable error when fetching data from YouTube API.
		 *
		 * @example
		 * await this.fetchVideosFromYouTube(chunkedRows)
		 *
		 * @see {@link https://developers.google.com/youtube/v3/docs/videos/list#response|YouTube Data API Response}
		 */
		async fetchVideosFromYouTube(chunkedRows) {
			const videoData = await Promise.all(
				chunkedRows.map(async (chunk) => {
					const videoIds = chunk.map((video) => video.videoId);

					return retry(
						async (bail) => {
							try {
								const response = await this.youtubeDataApi.listVideos({
									part: ["statistics", "snippet"],
									id: videoIds,
								});
								return response;
							} catch (error) {
								if (error.code === 400) {
									console.error("Bad request error:", error.message);
									bail(new Error(`Bad request: ${error.message}`));
								} else if (error.code === 403) {
									console.error("Forbidden error:", error.message);
									bail(new Error(`Forbidden: ${error.message}`));
								} else if (error.code === 404) {
									console.warn("Video not found:", error.message);
									return { data: { items: [] } }; // Return empty array for not found videos
								} else if (error.code >= 500) {
									console.warn(`Server error (${error.code}):`, error.message);
									throw error; // Retry on server errors
								} else {
									console.error("Unknown error:", error);
									bail(new Error(`Unknown error: ${error.message}`));
								}
							}
						},
						{
							retries: 3,
							factor: 2,
							minTimeout: 1000,
							maxTimeout: 5000,
							onRetry: (error, attempt) => {
								console.log(`Attempt ${attempt} failed. Retrying...`);
							},
						}
					);
				})
			);

			const videos = videoData
				.filter(
					(response) =>
						response && response.data && Array.isArray(response.data.items)
				)
				.map((response) => response.data.items)
				.flat();

			return videos;
		},
		/**
		 * @typedef {Object} Thumbnail
		 * @property {string} url - The URL of the thumbnail image.
		 * @property {number} width - The width of the thumbnail in pixels.
		 * @property {number} height - The height of the thumbnail in pixels.
		 */

		/**
		 * Retrieves the URL of the highest resolution thumbnail available.
		 *
		 * @param {Object} thumbnails - An object containing thumbnail data at various resolutions. May include any of: maxres, standard, high, medium, default.
		 * @param {Thumbnail} [thumbnails.maxres] - Maximum resolution thumbnail.
		 * @param {Thumbnail} [thumbnails.standard] - Standard resolution thumbnail.
		 * @param {Thumbnail} [thumbnails.high] - High resolution thumbnail.
		 * @param {Thumbnail} [thumbnails.medium] - Medium resolution thumbnail.
		 * @param {Thumbnail} [thumbnails.default] - Default (lowest) resolution thumbnail.
		 *
		 * @param {string} title - The title of the video, used for logging if no thumbnail is found.
		 *
		 * @returns {string|null} The URL of the highest resolution thumbnail available, or null if no thumbnail is found.
		 *
		 * @example
		 * const thumbnails = {
		 *   default: { url: "https://example.com/default.jpg", width: 120, height: 90 },
		 *   medium: { url: "https://example.com/medium.jpg", width: 320, height: 180 },
		 *   high: { url: "https://example.com/high.jpg", width: 480, height: 360 },
		 *   standard: { url: "https://example.com/standard.jpg", width: 640, height: 480 },
		 *   maxres: { url: "https://example.com/maxres.jpg", width: 1280, height: 720 }
		 * };
		 * const url = this.getThumbnailURL(video.snippet.thumbnails, video.snippet.title);
		 * // Returns: "https://example.com/maxres.jpg"
		 */
		getThumbnailURL(thumbnails, title) {
			const resolutions = ["maxres", "standard", "high", "medium", "default"];

			for (let resolution of resolutions) {
				if (thumbnails[resolution] && thumbnails[resolution].url) {
					return thumbnails[resolution].url;
				}
			}

			console.log(`No thumbnail found for video: ${title}`);
			return null;
		},
		/**
		 * Builds an array of updated objects combining data from Notion rows and YouTube video data.
		 *
		 * @param {Array<Object>} rows - The flattened array of simplified objects with the Notion Page ID, YouTube Video ID, and URL.
		 * @param {Array<Object>} videoData - An array of video objects from the YouTube Data API, each containing the video's statistics and snippet data.
		 * @returns {Array<Object>} An array of updated objects with the Notion Page ID, YouTube Video ID, URL, view count, like count, comment count, publish date, YouTube title, and thumbnail URL (if found).
		 *
		 * @example
		 * const rows = [{ id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', videoId: 'uvkmvXbEfec', url: 'https://www.youtube.com/watch?v=uvkmvXbEfec' }];
		 * const videoData = [{ id: 'uvkmvXbEfec', statistics: { viewCount: '56408', likeCount: '1635', commentCount: '60' }, snippet: { publishedAt: '2024-06-03T17:15:22Z', title: '8 New Notion Features You Should Know About!', thumbnails: { ... } } }];
		 * const result = this.buildUpdateArray(rows, videoData);
		 * // Result:
		 * // [{
		 * //   id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
		 * //   videoId: 'uvkmvXbEfec',
		 * //   url: 'https://www.youtube.com/watch?v=uvkmvXbEfec',
		 * //   views: 56408,
		 * //   likes: 1635,
		 * //   comments: 60,
		 * //   publish: '2024-06-03T17:15:22Z',
		 * //   ytTitle: '8 New Notion Features You Should Know About!',
		 * //   thumbnail: 'https://i.ytimg.com/vi/uvkmvXbEfec/maxresdefault.jpg'
		 * // }]
		 */
		buildUpdateArray(rows, videoData) {
			const videoIdSet = new Set(videoData.map((video) => video.id));
			const filteredRows = rows.filter((row) => videoIdSet.has(row.videoId));

			console.log("Rows:" + rows.length);
			console.log(
				"Filtered rows (after checking each page's video ID against the set of video IDs returned by YouTube Data API):" +
					filteredRows.length
			);
			console.dir(filteredRows, { depth: null });

			const updatedRows = filteredRows
				.map((row) => {
					const video = videoData.find((video) => video.id === row.videoId);
					if (!video) {
						console.log(`No video data found for video ID: ${row.videoId}`);
						return null;
					}
					const thumbnailUrl = video.snippet.thumbnails
						? this.getThumbnailURL(
								video.snippet.thumbnails,
								video.snippet.title
						  )
						: null;
					return {
						id: row.id,
						videoId: row.videoId,
						url: row.url,
						views: parseInt(video.statistics.viewCount, 10) || 0,
						likes: parseInt(video.statistics.likeCount, 10) || 0,
						comments: parseInt(video.statistics.commentCount, 10) || 0,
						publish: video.snippet.publishedAt,
						ytTitle: video.snippet.title,
						thumbnail: thumbnailUrl,
					};
				})
				.filter(Boolean);

			return updatedRows;
		},
		/**
		 * Updates the properties of each Notion page with the view count, like count, comment count, and publish date. Also updates the thumbnail, if a thumbnail was found.
		 *
		 * @param {import('@notionhq/client').Client} notion
		 * @param {Array<Object>} updatedRows - An array of updated objects with the Notion Page ID, YouTube Video ID, URL, view count, like count, comment count, publish date, YouTube title, and thumbnail URL (if found).
		 * @returns {Array<string>} An array of Notion Page IDs that were successfully updated.
		 *
		 * @example
		 * await this.updateNotionPage(notion, updatedRows)
		 */
		async updateNotionPage(notion, updatedRows) {
			// Set up our Bottleneck limiter
			const limiter = new Bottleneck({
				minTime: this.disableRateBursting === true ? 333 : 10,
				maxConcurrent: this.disableRateBursting === true ? 1 : 20,
			});

			// Handle 429 errors
			limiter.on("error", (error, jobInfo) => {
				const isRateLimitError = error.statusCode === 429;
				if (isRateLimitError) {
					console.log(
						`Job ${jobInfo.options.id} failed due to rate limit: ${error}`
					);
					const waitTime = error.headers["retry-after"]
						? parseInt(error.headers["retry-after"], 10)
						: 0.4;
					console.log(`Retrying after ${waitTime} seconds...`);
					return waitTime * 1000;
				}

				console.log(`Job ${jobInfo.options.id} failed: ${error}`);
				// Don't retry via limiter if it's not a 429
				return;
			});

			const updater = async (row) => {
				return retry(
					async (bail) => {
						try {
							const props = {
								[this.viewCount]: {
									number: row.views,
								},
								...(this.likeCount && {
									[this.likeCount]: {
										number: row.likes,
									},
								}),
								...(this.commentCount && {
									[this.commentCount]: {
										number: row.comments,
									},
								}),
								...(this.publishDate && {
									[this.publishDate]: {
										date: {
											start: row.publish,
										},
									},
								}),
							};

							const updateParams = {
								page_id: row.id,
								properties: props,
							}

							if (this.videoTitle && this.videoTitle !== "" && this.updateTitle === true) {
								const titleProp = JSON.parse(this.videoTitle)
								const title = row.ytTitle

								updateParams.properties[titleProp.name] = {
									[titleProp.type]: [
										{
											type: "text",
											text: {
												content: title,
											},
										},
									],
								}
							}

							if (this.setThumbnail === true && row.thumbnail) {
								updateParams.cover = {
									type: "external",
									external: {
										url: row.thumbnail,
									},
								}
							}

							await notion.pages.update(updateParams);

							return row.id;
						} catch (error) {
							if (400 <= error.status && error.status <= 409) {
								// Don't retry for errors 400-409
								bail(error);
								return;
							}
						}
					},
					{
						retries: 2,
						onRetry: (error, attempt) => {
							console.log(
								`Attempt ${attempt} failed with error message ${error}. Retrying...`
							);
						},
					}
				);
			};

			const updatedNotionPages = await Promise.all(
				updatedRows.map((row) => limiter.schedule(() => updater(row)))
			);

			return updatedNotionPages;
		},
	},
	async run({ steps, $ }) {
		const notion = new Client({ auth: this.notion.$auth.oauth_access_token });

		/* Log rate-bursting status */
		if (this.disableRateBursting === true) {
			console.log(`Rate bursting is disabled. Script will update Notion pages using established Notion API rate guidance of 3 requests per second.`)
		} else {
			console.log(`Rate bursting is enabled. Script will update Notion pages as fast as possible, and will attempt to retry failed requests using wait-until headers. If you are experiencing rate limiting issues, consider enabling the "Disable Rate Bursting" option.`)
		}
		
		/* Fetch videos from Notion */
		console.log(
			`Fetching all pages from connected Notion database that have a YouTube URL...`
		);
		const rows = await this.fetchVidsFromNotion(notion);
		console.log("Pages fetched from Notion:" + rows.length);
		console.dir(rows, { depth: null });

		/* Create arrays of objects with Notion Page ID, YouTube URL, and YouTube ID. Each array can have up to 50 objects to comply with YouTube API limits. Place all arrays into a containing array. */
		console.log(`Building arrays of video objects...`);
		const chunkedRows = this.buildVideoArray(rows);
		console.log(
			"Multi-dimensional array of video objects:" + chunkedRows.length
		);
		console.dir(chunkedRows, { depth: null });

		/* For each array of video objects, fetch video data from the YouTube Data API */
		console.log(`Fetching video data from the YouTube Data API...`);
		const videoData = await this.fetchVideosFromYouTube(chunkedRows);
		console.log("Video data:" + videoData.length);
		console.dir(videoData, { depth: null });

		/* Add the view count, like count, comment count, and maxrres thumbnail URL to each object in chunkedRows */
		console.log(
			`Filtering out any pages that don't have a valid response from the YouTube Data API, then constructing an updated array of video data...`
		);
		const updatedRows = this.buildUpdateArray(chunkedRows.flat(), videoData);
		console.log("Updated rows:" + updatedRows.length);
		console.dir(updatedRows, { depth: null });

		/* Update the pages in Notion */
		console.log(`Updating Notion pages...`);
		const updatedNotionPages = await this.updateNotionPage(notion, updatedRows);
		console.log("Updated Notion pages:" + updatedNotionPages.length);
		console.dir(updatedNotionPages, { depth: null });

		return updatedNotionPages;
	},
};
