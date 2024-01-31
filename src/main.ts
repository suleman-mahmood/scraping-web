import { Dataset, PlaywrightCrawler } from 'crawlee';

const START_CB_RANK = 0;
const BASEL_URL = 'https://www.crunchbase.com/search/organization.companies/88dfccdbbba953493e8754668e3485ce';

const crawler = new PlaywrightCrawler({
    async requestHandler({ request, page, enqueueLinks, log, pushData }) {
        if (request.label === 'initial') {
            let nextRank = START_CB_RANK.toString();

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

            const label = `next-${nextRank}`;

            crawler.addRequests([{
                url: BASEL_URL,
                label: label,
                uniqueKey: label,
            }]);
        }
        else if (request.label?.includes('next')) {
            let nextRank = request.label.split('-')[1];

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
            const label = `stop-${nextRank}`;

            crawler.addRequests([{
                url: BASEL_URL,
                label: label,
                uniqueKey: label,
            }]);
        }
    },
    headless: false,
    // requestHandlerTimeoutSecs: 60 * 10, // 10 mins
    maxRequestRetries: 0,
});

await crawler.addRequests([{
    url: BASEL_URL,
    label: 'initial',
}])

await crawler.run([]);

const dataset = await Dataset.open('default');
await dataset.exportToCSV('results');
