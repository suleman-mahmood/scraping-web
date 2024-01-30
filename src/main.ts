import { Dataset, PlaywrightCrawler } from 'crawlee';

const START_CB_RANK = 0;

const crawler = new PlaywrightCrawler({
    async requestHandler({ request, page, enqueueLinks, log, pushData }) {
        let nextRank = START_CB_RANK.toString();

        for await (const i of [...Array(3000000 / 15).keys()]) {
            await page.fill('.mat-mdc-input-element', nextRank);
            await page.click('search-button .mdc-button');

            await page.waitForSelector('.identifier-label');

            for await (const i of [...Array(15).keys()]) {
                const orgName = await page.locator('.identifier-label').nth(i).innerText();

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
    requestHandlerTimeoutSecs: 30,
    // requestHandlerTimeoutSecs: 60 * 10, // 10 mins
    maxRequestRetries: 0,
});

await crawler.run(['https://www.crunchbase.com/search/organization.companies/88dfccdbbba953493e8754668e3485ce']);

const dataset = await Dataset.open('default');
await dataset.exportToCSV('results');
