import { BrowserName, Dataset, DeviceCategory, OperatingSystemsName, PlaywrightCrawler, ProxyConfiguration, SessionPool } from 'crawlee';

// 3.2 million companies
// 10k rows in 10 mins
// 3.2 million / 10k = 320 times
// 320 * 10 mins = 3200 mins = 53.3 hours
// So we need 100 scrapers to run for 1 hour to get all the data with duplication
// Start points in intervals of 100, 3.2 million / 100 = 32k

const BASEL_URL = 'https://www.crunchbase.com/search/organization.companies/88dfccdbbba953493e8754668e3485ce';

const DEBUG_RUN = true;

const TOTAL_REQUESTS = DEBUG_RUN ? 1 : 100;
const ROWS_TO_SCRAPE = DEBUG_RUN ? 320 : 32000;
const ROWS_IN_ONE_PAGE = 15;

const crawler = new PlaywrightCrawler({
    async requestHandler({ request, page, enqueueLinks, log, pushData }) {
        let nextRank = request.label?.split('-')[1] ?? '0';

        for await (const i of [...Array(Math.ceil(ROWS_TO_SCRAPE / ROWS_IN_ONE_PAGE)).keys()]) {
            // generate random number between 500 and 3000
            // const randomMs = Math.floor(Math.random() * 2500) + 500;
            // await page.waitForTimeout(randomMs);

            await page.fill('.mat-mdc-input-element', nextRank);
            await page.click('search-button .mdc-button');

            await page.waitForSelector('.identifier-label');

            for await (const i of [...Array(ROWS_IN_ONE_PAGE).keys()]) {
                const orgName = await page.locator('.identifier-label').nth(i).innerText();

                const orgLinkCol = await page.locator('grid-cell.column-id-identifier').nth(i);
                const orgLink = await orgLinkCol.locator('a').getAttribute('href');

                const firstCol = await page.locator('grid-cell.column-id-categories').nth(i);
                const industries = await firstCol.locator('identifier-multi-formatter .ng-star-inserted').allInnerTexts();

                const secondCol = await page.locator('grid-cell.column-id-location_identifiers').nth(i);
                const headquarterLocation = await secondCol.locator('identifier-multi-formatter .ng-star-inserted').allInnerTexts();

                const thirdCol = await page.locator('grid-cell.column-id-short_description').nth(i);
                const description = await thirdCol.locator('span').innerText();

                const fourthCol = await page.locator('grid-cell.column-id-rank_org_company').nth(i);
                const cbRank = await fourthCol.locator('a').innerText();

                const results = {
                    organization_name: orgName,
                    organization_link: orgLink,
                    industries: industries,
                    headquarterLocation: headquarterLocation,
                    description: description,
                    cbRank: cbRank,
                };
                await pushData(results);

                nextRank = cbRank;
            }
        }
    },
    headless: false,
    requestHandlerTimeoutSecs: 60 * 10, // 10 mins
    maxRequestRetries: 0,
    maxConcurrency: 1,

    // maxConcurrency: 5,
    // useSessionPool: true,
    // sessionPoolOptions: {
    //     sessionOptions: {
    //         maxUsageCount: 1,
    //     }
    // },
    // proxyConfiguration: new ProxyConfiguration({
    //     proxyUrls: [
    //         // "http://50.172.218.164:80",
    //         // "http://94.241.173.37:8080",
    //         // "http://155.94.241.131:3128",
    //         // "http://72.10.164.178:10801",
    //         // "http://78.28.152.111:80",
    //         // "http://116.203.28.43:80",
    //         // "http://183.100.14.134:8000",
    //         // "http://62.210.114.201:8080",
    //         // "http://13.40.247.115:80",
    //         // "http://51.15.242.202:8888",
    //         // "http://114.129.2.82:8081",
    //         // "http://207.2.120.16:80",
    //         // "http://216.137.184.253:80",
    //         // "http://82.146.37.145:80",
    //         // "http://46.47.197.210:3128",
    //         // "http://50.218.57.71:80",
    //         // "http://198.176.56.43:80",
    //         // "http://51.159.0.236:2020",
    //         "http://156.244.64.160:40183",
    //         "http://103.168.155.116:80",
    //         "http://207.2.120.15:80",
    //         // "http://50.237.207.186:80",
    //         // "http://195.114.209.50:80",
    //         // "http://154.65.39.7:80",
    //         // "http://138.197.148.215:80",
    //         // "http://103.86.1.22:4145",
    //         // "http://178.128.49.205:80",
    //         // "http://14.207.3.159:5678",
    //         // "http://96.70.52.227:48324",
    //         // "http://41.225.229.55:1080",
    //     ],
    // }),
    // browserPoolOptions: {
    //     maxOpenPagesPerBrowser: 2,
    //     retireBrowserAfterPageCount: 5,
    //     fingerprintOptions: {
    //         fingerprintGeneratorOptions: {
    //             browsers: [BrowserName.firefox, BrowserName.chrome, BrowserName.edge, BrowserName.safari],
    //             devices: [DeviceCategory.desktop],
    //             operatingSystems: [OperatingSystemsName.windows, OperatingSystemsName.macos, OperatingSystemsName.linux],
    //         },
    //     },
    // },
    // navigationTimeoutSecs: 60 * 2, // 2 mins
});

const allRequests = [];

for (const i of [...Array(TOTAL_REQUESTS).keys()]) {
    const startPoint = i * ROWS_TO_SCRAPE;
    const label = `initial-${startPoint}`;

    allRequests.push({
        url: BASEL_URL,
        label: label,
        uniqueKey: label,
    });
}

await crawler.addRequests(allRequests);
await crawler.run();

// Save all the data to CSV
const dataset = await Dataset.open('default');
await dataset.exportToCSV('results');
