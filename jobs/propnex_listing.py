import time
import random
import csv
import datetime
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

class PropnexListingSpider:
    def __init__(self, driver_url, options, base_url, page_number, output_file):
        self.driver = webdriver.Remote(command_executor=driver_url, options=options)
        self.base_url = base_url
        self.page_number = page_number
        self.current_url = f'{self.base_url}&pageNumber={self.page_number}'
        self.output_file = output_file
        
        # Initialize the output CSV file
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['url'])  # Write the header row

    def scrape_listings(self):
        
        while True:
            # Load the current page
            print(f'Scraping page {self.page_number}: {self.current_url}')
            self.driver.get(self.current_url)
            
            # Check if the 'Listing not found' message is present
            try:
                not_found_message = self.driver.find_element(By.CSS_SELECTOR, 'h4[style="margin-top:25px"]')
                if not_found_message and 'Listing not found' in not_found_message.text:
                    print('No more listings found. Exiting...')
                    break
            except Exception as e:
                pass  # Continue if the message is not found
            
            # Wait for all the listings to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'div.col-lg-6.col-sm-6'))
                )
            except Exception as e:
                print(f'Error: {e}')
                break

            time.sleep(1)

            # Scroll to simulate human action
            self.scrolling_action()

            # Get all the listings on the page
            properties = self.driver.find_elements(By.CSS_SELECTOR, 'div.col-lg-6.col-sm-6')
            property_urls = []

            count = 0 # to remove

            # Collect all the URLs from the available listings
            for property_item in properties:
                try:
                    property_li_items = property_item.find_elements(By.CSS_SELECTOR, 'li a')
                    hrefs = [item.get_attribute('href') for item in property_li_items]
                    relative_url = hrefs[2]
                except Exception as e:
                    print('Nothing found...')
                    continue

                # Ensure all URLs are in the correct format
                if 'www.propnex.com/listing-details' not in relative_url:
                    property_url = 'https://www.propnex.com/listing-details' + relative_url
                else:
                    property_url = relative_url

                print(property_url)
                property_urls.append(property_url)
                
                count += 1 # to remove
                if count == 3:
                    break

            # Save the URLs to the CSV file
            self.save_urls_to_csv(property_urls)

            # Check if no listings were found
            if not property_urls:
                print(f'No listings found on page {self.page_number}. Exiting...')
                break
            
            if self.page_number == 1: # to remove
                break

            # Move to the next page
            self.page_number += 1
            self.current_url = f'{self.base_url}&pageNumber={self.page_number}'
            time.sleep(2)

    def save_urls_to_csv(self, urls):
        # Append the URLs to the output CSV file
        with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for url in urls:
                writer.writerow([url])

    def scrolling_action(self):
        # Simulate random scrolling on the page
        scrolling_times = random.randint(2, 4)
        scrolling_pixels = [2000, 3000]
        pixel = random.choice(scrolling_pixels)
        for i in range(scrolling_times):
            if i % 2 == 0:
                ActionChains(self.driver).scroll_by_amount(0, pixel).perform()
            else:
                ActionChains(self.driver).scroll_by_amount(0, -pixel).perform()
            time.sleep(2)

    def quit(self):
        # Close the browser when done
        self.driver.quit()


def propnex_listing():
    driver_url = 'http://selenium-chrome:4444/wd/hub'
    options = webdriver.ChromeOptions()
    base_url = 'https://www.propnex.com/buy?propertyType=HDB&sortBy=newest&listingType=SALE&condoPropertyType=CONDO%2CAPT'
    page_number = 1
    os.makedirs('/opt/airflow/result/propnex/', exist_ok=True)
    output_file_path = f'/opt/airflow/result/propnex/{datetime.datetime.today().date()}_propnex_listing.csv'
    output_file = f'{datetime.datetime.today().date()}_propnex_listing.csv'
    spider = PropnexListingSpider(driver_url, options, base_url, page_number, output_file_path)
    
    try:
        spider.scrape_listings()  # Start scraping
    finally:
        spider.quit()

    return output_file