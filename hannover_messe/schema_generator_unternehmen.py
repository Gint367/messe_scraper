import asyncio
import json
from pprint import pprint
from crawl4ai import (
    AsyncWebCrawler,
    CrawlerRunConfig,
    BrowserConfig,
    JsonCssExtractionStrategy,
    JsonXPathExtractionStrategy,
    LLMConfig,
)

js_code_erp = """
        (async () => {
            // 1. Enter "erp" in the search box
            const searchInput = document.querySelector('input[name="search"]');
            if (searchInput) {
                searchInput.value = 'erp';
            } else {
                console.error('Search input not found');
                return; // Stop execution if search input is not found
            }

            // 2. Click the search button
            const searchButton = document.querySelector('button[type="submit"].search-bar-submit');
            if (searchButton) {
                searchButton.click(); // Directly click the button
            } else {
                console.error('Search button not found');
                return;
            }

            // Wait for the search results to load (Replace Existing Content - EXAMPLE)
            await new Promise(resolve => {
                const observer = new MutationObserver(mutations => {
                    for (let mutation of mutations) {
                        if (mutation.type === 'childList' && mutation.addedNodes.length > 0) {
                            // Check if the added node is the search results container
                            for (let node of mutation.addedNodes) {
                                if (node.classList && node.classList.contains('search-results-list')) {
                                    observer.disconnect();
                                    resolve();
                                    console.log("Search results loaded (Replace Existing Content)");
                                    return;
                                }
                            }
                        }
                    }
                });

                observer.observe(document.querySelector('.search-results-content'), { // Or the parent of the results
                    childList: true,
                    subtree: false // Only observe direct children
                });
            });


            // 3. Click the "Sprecher & Events" filter (data-value="ep")
            const sprecherEventsLink = document.querySelector('a[data-value="ep"]');
            if (sprecherEventsLink) {
                sprecherEventsLink.click();
            } else {
                console.error('Sprecher & Events link not found');
                return;
            }

            // Wait for the "Sprecher & Events" filter to load.
            await new Promise(resolve => {
                const observer = new MutationObserver(mutations => {
                    const EvSearchResultFound = document.querySelector('.search-results-list');
                    if (EvSearchResultFound) {
                        observer.disconnect();
                        resolve();
                    }
                });

                observer.observe(document.body, {
                    childList: true,
                    subtree: true
                });
            });

            // 4. Click the "Treffertyp" header to reveal the checkboxes
            const treffertypHeader = document.querySelector('h3.t.set-200-bold.closed');
            if (treffertypHeader) {
                treffertypHeader.click();
            } else {
                console.error('Treffertyp header not found');
                return;
            }

            // Wait for the "Treffertyp" checkboxes to become visible.  Adapt the condition!
             await new Promise(resolve => {
                const observer = new MutationObserver(mutations => {
                    const checkboxesVisible = document.querySelector('div[data-value="Aussteller"]'); // Or a more specific selector
                    if (checkboxesVisible) {
                        observer.disconnect();
                        resolve();
                    }
                });

                observer.observe(document.body, {
                    childList: true,
                    subtree: true
                });
            });

            // 5. Check the "Aussteller" checkbox
            const ausstellerCheckboxDiv = document.querySelector('div[data-value="Aussteller"]');
            if (ausstellerCheckboxDiv) {
                const ausstellerCheckbox = ausstellerCheckboxDiv.querySelector('input[type="checkbox"].mdc-checkbox__native-control');

                if (ausstellerCheckbox && !ausstellerCheckbox.checked) {
                    ausstellerCheckbox.click();  // Simulate a click on the checkbox
                }
             else {
                console.error("checkbox already clicked")
             }
            }
            else {
                console.error('Aussteller checkbox not found');
                return;
            }

            // Wait for the filter to apply and the results to update.
            // Adapt this condition to the specific page structure
            await new Promise(resolve => {
                const observer = new MutationObserver(mutations => {
                    const filteredResultFound = document.querySelector('.search-results-list');
                    if (filteredResultFound) {
                        observer.disconnect();
                        resolve();
                    }
                });

                observer.observe(document.body, {
                    childList: true,
                    subtree: true
                });
            });

            console.log("Finished Javascript execution.");
        })();
        """

async def getSchema():
    # JavaScript to enter "erp" in the search box, click the search button,
    # and then checkbox "Aussteller"
    

    browserConfig = BrowserConfig(headless=True, extra_args=["--disable-web-security"])
    async with AsyncWebCrawler(config=browserConfig) as crawler:
        result = await crawler.arun(
            url="https://www.hannovermesse.de/de/suche/",
            config=CrawlerRunConfig(
                #js_code=js_code_erp,
                delay_before_return_html=2.0,
                scan_full_page=True,
                scroll_delay=0.7,
            ),
        )
        schema = JsonXPathExtractionStrategy.generate_schema(
            result.html,
            llm_config=LLMConfig(
                provider="gemini/gemini-2.0-flash",
            ),
            target_json_example={
                "company": [
                    {
                        "name": "BDE-Engineering",
                        "location": "Beverungen - DE",
                        "description": "... Die <strong>BDE</strong> Engineering ist ein innovatives Unternehmen im Bereich Fertigungsmanagementsysteme und bietet Lösungen ...",
                        "stand": "Halle 15, Stand A18, (7)",
                    },
                    {
                        "name": "AMM Systems",
                        "location": "Johannesburg - ZA",
                        "description": "AM Squared Systems bietet Echtzeit-Datenmanagementlösungen für die Energie-, Bergbau- und Fertigungsindustrien. Unsere MiX-Plattform integriert ...",
                        "stand": "Halle 16, Stand E11",
                    },
                ],
            },
        )
        return schema


async def main(schema):
    browserConfig = BrowserConfig(headless=True, extra_args=["--disable-web-security"])
    config = CrawlerRunConfig(
        extraction_strategy=JsonCssExtractionStrategy(schema=schema),
        js_code=js_code_erp,
        delay_before_return_html=10.0,
        scan_full_page=True,
        scroll_delay=0.7,
    )
    async with AsyncWebCrawler(config=browserConfig) as crawler:
        result = await crawler.arun(
            url="https://www.hannovermesse.de/de/suche/",
            config=config,
        )
        data = json.loads(result.extracted_content)
        # Save the extracted data to a JSON file
        with open("hannover_messe_results.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Data saved to hannover_messe_results.json with {len(data)} results")

        # Also print the first few entries for quick verification
        print("\nSample of extracted data:")
        for i, item in enumerate(data[:3]):  # Show first 3 items
            print(f"\nItem {i+1}:")
            pprint(item)
        


async def test_crawl():
    # Use this one time
    #schema = await getSchema()
    #pprint(schema)
    schema_json = {
        "baseSelector": ".o.search.snippet.module-theme-100",
        "fields": [
            {
                "name": "company_name",
                "selector": ".t.set-300-bold.as-headline.search-snippet-name",
                "type": "text",
            },
            {
                "name": "location",
                "selector": ".t.set-040-wide.as-copy.search-snippet-attribute",
                "type": "text",
            },
            {
                "name": "description",
                "selector": ".t.set-200-regular.as-copy.search-snippet-description",
                "type": "text",
            },
            {
                "name": "stand",
                "selector": ".t.set-100-regular.as-copy.search-snippet-location",
                "type": "text",
            },
            {
                "attribute": "href",
                "name": "product_link",
                "selector": "a.o.link.as-block.fx.dropshadow.for-child",
                "type": "attribute",
            },
            {
                "name": "search_snippet_type",
                "selector": ".t.set-040-caps.as-caption.search-snippet-type",
                "type": "text",
            },
        ],
        "name": "Hannover Messe Search Results",
    }
    schema_xpath = {
        "baseSelector": ".search-snippet",
        "fields": [
            {
                "attribute": "href",
                "name": "link",
                "selector": "a.o.link",
                "type": "attribute",
            },
            {"name": "type", "selector": ".search-snippet-type", "type": "text"},
            {"name": "name", "selector": ".search-snippet-name", "type": "text"},
            {
                "name": "location",
                "selector": ".search-snippet-attribute",
                "type": "text",
            },
            {
                "name": "description",
                "selector": ".search-snippet-description",
                "type": "text",
            },
            {"name": "stand", "selector": ".search-snippet-location", "type": "text"},
        ],
        "name": "Hannover Messe Search Results",
    }
    await main(schema_json)


if __name__ == "__main__":
    asyncio.run(test_crawl())
