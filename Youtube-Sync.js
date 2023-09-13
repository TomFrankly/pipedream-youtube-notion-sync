import { Client } from "@notionhq/client";
import Bottleneck from "bottleneck";
import retry from "async-retry";
import youtubeDataApi from "@pipedream/youtube_data_api";

export default {
	name: "YouTube to Notion – View Counts",
	description:
		"Fetches view, like, and comment counts for each YouTube video in a Notion database. Uses the public YouTube Data API.",
	key: "youtube-notion-sync-views",
	version: "0.2.6",
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
			(k) => properties[k].type === "title"
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
			videoTitle: {
				type: "string",
				label: "Video Title (Required)",
				description: "Select the title property for your videos.",
				options: titleProps.map((prop) => ({ label: prop, value: prop })),
				optional: false,
			},
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
				description: "If you want to automatically update your publish dates so they are accurate, select your Publish Date property. Otherwise, leave it blank.",
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
		};

		return props;
	},
	methods: {
		async fetchVidsFromNotion(notion) {
			// Pagination variables
			let hasMore = undefined;
			let token = undefined;

			// Set up our Bottleneck limiter
			const limiter = new Bottleneck({
				minTime: 333,
				maxConcurrent: 1,
			});

			// Handle 429 errors
			limiter.on("error", (error) => {
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

			// Initial array for arrays of User or Project objects
			let rows = [];

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
								rows.push(resp.results);

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

			return rows.flat();
		},
		getVideoId(url) {
			const videoIdRegex =
				/(?:youtube(?:-nocookie)?\.com\/(?:[^/\n\s]+\/\S+\/|(?:v|vi|e(?:mbed)?)\/|\S*?[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
			const match = url.match(videoIdRegex);
			return match ? match[1] : null;
		},
		buildVideoArray(rows) {
			const fullVideoArray = rows.map((row) => {
				const url = row.properties[this.videoURL].url;
				const videoId = this.getVideoId(url);
				return {
					id: row.id,
					videoId: videoId,
					url: url,
				};
			});

			// Split the returned array into chunks of 50 videos
			const chunkedVideoArray = fullVideoArray.reduce(
				(resultArray, item, index) => {
					const chunkIndex = Math.floor(index / 50);
					if (!resultArray[chunkIndex]) {
						resultArray[chunkIndex] = [];
					}
					resultArray[chunkIndex].push(item);
					return resultArray;
				},
				[]
			);

			return chunkedVideoArray;
		},
		async fetchVideosFromYouTube(chunkedRows) {
			const videoData = await Promise.all(
				chunkedRows.map(async (chunk) => {
					const videoIds = chunk.map((video) => video.videoId);
					return await this.youtubeDataApi.listVideos({
						part: ["statistics", "snippet"],
						id: videoIds,
					});
				})
			);

			const videos = videoData.map((video) => video.data.items).flat();

			return videos;
		},
		buildUpdateArray(rows, videoData) {

			const videoIdSet = new Set(videoData.map((video) => video.id));
			const filteredRows = rows.filter((row) => videoIdSet.has(row.videoId))

			console.log("Filtered rows:");
			console.dir(filteredRows, { depth: null });

			const updatedRows = filteredRows.map((row) => {
				const video = videoData.find((video) => video.id.includes(row.videoId));
				return {
					id: row.id,
					videoId: row.videoId,
					url: row.url,
					views: parseInt(video.statistics.viewCount),
					likes: parseInt(video.statistics.likeCount),
					comments: parseInt(video.statistics.commentCount),
					publish: video.snippet.publishedAt,
					ytTitle: video.snippet.title,
					thumbnail: video.snippet.thumbnails.maxres.url,
				};
			});
			return updatedRows;
		},
		async updateNotionPage(notion, updatedRows) {
			// Set up our Bottleneck limiter
			const limiter = new Bottleneck({
				minTime: 333,
				maxConcurrent: 1,
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

							const cover = {
								type: "external",
								external: {
									url: row.thumbnail,
								},
							};

							await notion.pages.update({
								page_id: row.id,
								properties: props,
								...(this.setThumbnail === true && {
									cover: cover,
								}),
							});

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

		const rows = await this.fetchVidsFromNotion(notion);
		console.log("Rows from Notion:" + rows.length);
		console.dir(rows, { depth: null });

		const chunkedRows = this.buildVideoArray(rows);
		console.log("Chunked rows:" + chunkedRows.length);
		console.dir(chunkedRows, { depth: null });

		const videoData = await this.fetchVideosFromYouTube(chunkedRows);
		console.log("Video data:" + videoData.length);
		console.dir(videoData, { depth: null });

		// Add the view count, like count, comment count, and maxrres thumbnail URL to each object in chunkedRows
		const updatedRows = this.buildUpdateArray(chunkedRows.flat(), videoData);
		console.log("Updated rows:"	+ updatedRows.length);
		console.dir(updatedRows, { depth: null });

		// Update the pages in Notion
		const updatedNotionPages = await this.updateNotionPage(notion, updatedRows);
		console.log("Updated Notion pages:" + updatedNotionPages.length);
		console.dir(updatedNotionPages, { depth: null });

		return updatedNotionPages;
	},
};
