import { Dataset, Log, PlaywrightCrawler, Source } from 'crawlee';
import { Page } from 'playwright';
import { chromium } from 'playwright-extra';
import stealthPlugin from 'puppeteer-extra-plugin-stealth';

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

    13 failures with 12 requests per minute for 100 requests and done in 19 mins
*/

/* Config */

const BASEL_URL = 'https://www.crunchbase.com';
const SEARCH_URL = `${BASEL_URL}/search/organization.companies`;

const DEBUG_RUN = true;

const START_OFFSET = 0;
const TOTAL_REQUESTS = DEBUG_RUN ? 1 : 100;
const ROWS_TO_SCRAPE = DEBUG_RUN ? 100 : 32000;
const ROWS_IN_ONE_PAGE = 15;

enum CaptchaType {
    fullscreen = 'fullscreen',
    overlay = 'overlay',
}
enum LocatorType {
    click = 'click',
    nthClick = 'nthClick',
    wait = 'wait',
    fill = 'fill',
}

const runId = Math.floor(Math.random() * 1000) + 1

/* Opening datasets */

const searchDataset = await Dataset.open(`${runId}-search-results`);
const orgDataset = await Dataset.open(`${runId}-org-results`);
const captchaLogs = await Dataset.open(`${runId}-captcha-logs`);

const solveCaptcha = async (page: Page, log: Log, type: CaptchaType, selector: string) => {
    /*
    Straregy:
        - Click location
        - Click duration
    */

    const screenSize = page.viewportSize()!;

    log.info('Trying to solve captcha', {
        screenSize: screenSize,
        type: type,
    });


    const clickLocation = {
        x: (screenSize.width / 2),
        y: (screenSize.height / 2)
            + (type === CaptchaType.fullscreen
                ? screenSize.height * 0.05
                : -1 * screenSize.height * 0.05
            ),
    }

    const delay = Math.floor(Math.random() * 11) + 5

    await page.mouse.click(clickLocation.x, clickLocation.y, {
        delay: 1000 * delay, // hold for 5-15 seconds
    });

    const scId = Math.floor(Math.random() * 10000) + 1

    page.screenshot({
        clip: {
            x: clickLocation.x - 200,
            y: clickLocation.y - 200,
            width: 400,
            height: 400,
        },
        path: `./screenshots/${runId}/captcha/${scId}.png`,
    });

    await captchaLogs.pushData({
        screenSize: screenSize,
        clickLocation: clickLocation,
        type: type,
        holdDuration: delay,
        scId: scId,
        selector: selector,
    })

    log.info('Captcha solved?');
}

const possiblySolveCaptcha = async (
    selector: string,
    page: Page,
    log: Log,
    locatorType: LocatorType,
    captchaType: CaptchaType = CaptchaType.overlay,
    nth: number = 0,
    fillText: string = '',
) => {
    try {
        if (locatorType === LocatorType.click) {
            await page.click(selector, { timeout: 1000 * 5, force: true });
        }
        else if (locatorType == LocatorType.wait) {
            await page.waitForSelector(selector, { timeout: 1000 * 5, state: 'attached' });
        }
        else if (locatorType == LocatorType.nthClick) {
            await page.locator(selector).nth(nth).click({ timeout: 1000 * 5, force: true });
        }
        else if (locatorType == LocatorType.fill) {
            await page.fill(selector, fillText, { timeout: 1000 * 5 });
        }
    } catch (error) {
        await solveCaptcha(page, log, captchaType, selector);

        if (locatorType === LocatorType.click) {
            await page.click(selector, { timeout: 1000 * 10, force: true });
        }
        else if (locatorType == LocatorType.wait) {
            await page.waitForSelector(selector, { timeout: 1000 * 10, state: 'attached' });
        }
        else if (locatorType == LocatorType.nthClick) {
            await page.locator(selector).nth(nth).click({ timeout: 1000 * 10, force: true });
        }
        else if (locatorType == LocatorType.fill) {
            await page.fill(selector, fillText, { timeout: 1000 * 10 });
        }
    }
}

/* Setting up our browser */
chromium.use(stealthPlugin());

const crawler = new PlaywrightCrawler({
    async requestHandler({ request, page, log }) {
        const pageType = request.label?.split('-')[0] ?? 'initial';

        if (pageType === 'initial') {
            await possiblySolveCaptcha('button.add-filter-button', page, log, LocatorType.click, CaptchaType.fullscreen);

            await possiblySolveCaptcha('input.mat-mdc-input-element', page, log, LocatorType.fill, CaptchaType.overlay, 0, 'rank');

            await possiblySolveCaptcha('button.mdc-list-item', page, log, LocatorType.nthClick, CaptchaType.overlay, 1);

            let nextRank = request.label?.split('-')[1] ?? '0';

            for await (const i of [...Array(Math.ceil(ROWS_TO_SCRAPE / ROWS_IN_ONE_PAGE)).keys()]) {
                await possiblySolveCaptcha('.mat-mdc-input-element', page, log, LocatorType.fill, CaptchaType.overlay, 0, nextRank);

                await possiblySolveCaptcha('search-button .mdc-button', page, log, LocatorType.click);

                await possiblySolveCaptcha('.identifier-label', page, log, LocatorType.wait);

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

                    await searchDataset.pushData(results);

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
            await possiblySolveCaptcha('fields-card', page, log, LocatorType.wait, CaptchaType.fullscreen);

            const results: any = {};
            results["url"] = request.url;

            const aboutCard = page.locator('fields-card').nth(0);
            const aboutFields = await aboutCard.locator('li').all();

            results["about-fields"] = [];

            for await (const field of aboutFields) {
                const data = await field.innerText();
                const links = await field.locator('a').all();

                const linksList = [];
                for await (const l of links) {
                    const link = await l.getAttribute('href');
                    linksList.push(link);
                }

                results["about-fields"].push({
                    "text": data,
                    "links": linksList,
                });
            }

            // await page.waitForSelector('nav a', { timeout: 1000 * 60, state: 'attached' });

            const tabs = await page.locator('nav a').all();
            for await (const t of tabs) {
                const text = await t.innerText();
                if (text === 'People') {
                    await t.click({ force: true });
                    break;
                }
            }

            await possiblySolveCaptcha('image-list-card', page, log, LocatorType.wait);

            results["people"] = [];

            const peopleCard = await page.locator('image-list-card li').all();
            for await (const p of peopleCard) {
                const text = await p.innerText();
                results["people"].push(text);
            }

            results['docText'] = await page.innerText('html');

            orgDataset.pushData(results);
        }
    },

    headless: false,
    // launchContext: {
    //     launcher: chromium,
    //     useIncognitoPages: true,
    //     launchOptions: {
    //         headless: false,
    //     },
    // },

    requestHandlerTimeoutSecs: 60 * 10, // 10 mins
    maxRequestRetries: 5,

    minConcurrency: 1,
    maxConcurrency: 1,

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

    maxRequestsPerMinute: 3,
});

const allRequests: Source[] = [];

for (const i of [...Array(START_OFFSET + TOTAL_REQUESTS).keys()]) {
    const startPoint = i * ROWS_TO_SCRAPE;
    const label = `initial-${startPoint}`;

    allRequests.push({
        url: SEARCH_URL,
        label: label,
        uniqueKey: label,
    });
}

console.log('Crawler started with run id:', runId);

// await crawler.addRequests([BASEL_URL]);
await crawler.addRequests(allRequests);
await crawler.run();

console.log('Crawler ended with run id:', runId);

// Save all the data to CSV
// await searchDataset.exportToCSV('search-results');
// await orgDataset.exportToCSV('org-results');
