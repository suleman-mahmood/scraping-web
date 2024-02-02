import { Dataset, PlaywrightCrawler, Source } from 'crawlee';

// 3.2 million companies
// 10k rows in 10 mins
// 3.2 million / 10k = 320 times
// 320 * 10 mins = 3200 mins = 53.3 hours
// So we need 100 scrapers to run for 1 hour to get all the data with duplication
// Start points in intervals of 100, 3.2 million / 100 = 32k

/* 
Config:
    - Parallel requests = 3
    - Total rows to scrape = 18000
    - Rows in one request = 6000

    Total time = 440 secs = 7.3 mins
    Time for 3.2 million rows = 3.2 million / 18000 * 440 = 7.3 * 177.7 = 1298.71 mins = 21.6 hours
*/

const BASEL_URL = 'https://www.crunchbase.com';
const SEARCH_URL = `${BASEL_URL}/search/organization.companies`;

const DEBUG_RUN = true;

const START_OFFSET = 0;
const TOTAL_REQUESTS = DEBUG_RUN ? 1 : 100;
const ROWS_TO_SCRAPE = DEBUG_RUN ? 30 : 32000;
const ROWS_IN_ONE_PAGE = 15;

const crawler = new PlaywrightCrawler({
    async requestHandler({ request, page, pushData }) {
        const pageType = request.label?.split('-')[0] ?? 'initial';

        if (pageType === 'initial') {
            await page.click('button.add-filter-button');
            await page.fill('input.mat-mdc-input-element', 'rank');
            await page.locator('button.mdc-list-item').nth(1).click();

            let nextRank = request.label?.split('-')[1] ?? '0';

            for await (const i of [...Array(Math.ceil(ROWS_TO_SCRAPE / ROWS_IN_ONE_PAGE)).keys()]) {
                await page.fill('.mat-mdc-input-element', nextRank);
                await page.click('search-button .mdc-button');

                await page.waitForSelector('.identifier-label');

                const orgUrls = [];

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
                    orgUrls.push(BASEL_URL + orgLink);

                    await pushData(results);

                    nextRank = cbRank;
                }

                // Time to scrape organizations
                const req = orgUrls.map((url) => {
                    const s: Source = {
                        url: url,
                        label: 'org-details',
                    }
                    return s;
                });
                await crawler.addRequests(req);
            }
        }
        else if (pageType === 'org') {
            await page.waitForTimeout(1000);
        }
    },
    headless: false,

    requestHandlerTimeoutSecs: 60 * 10, // 10 mins
    maxRequestRetries: 0,

    minConcurrency: 1,
    maxConcurrency: 2,

    useSessionPool: true,
    sessionPoolOptions: {
        blockedStatusCodes: [],
        sessionOptions: {
            maxUsageCount: 1,
        }    
    },
    autoscaledPoolOptions: {
        desiredConcurrency: 1,
    },
    browserPoolOptions: {
        maxOpenPagesPerBrowser: 1,
        retireBrowserAfterPageCount: 1,
    },
});

const allRequests = [];

for (const i of [...Array(START_OFFSET + TOTAL_REQUESTS).keys()]) {
    const startPoint = i * ROWS_TO_SCRAPE;
    const label = `initial-${startPoint}`;

    allRequests.push({
        url: SEARCH_URL,
        label: label,
        uniqueKey: label,
    });
}

await crawler.addRequests(allRequests);
await crawler.run();

// Save all the data to CSV
const dataset = await Dataset.open('default');
await dataset.exportToCSV('results');
