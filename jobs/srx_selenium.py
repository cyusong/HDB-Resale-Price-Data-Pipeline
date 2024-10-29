import time
import random
import json
import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

class SrxSpider:
    def __init__(self, driver_url, options, csv_path_to_urls):
        
        self.driver = webdriver.Remote(command_executor=driver_url, options=options)
        self.url_list = load_url_list(csv_path_to_urls)
        self.data = []

    def parse_property_page(self, driver):
        try:
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div#listing-detail')))
            listing_details = driver.find_element(By.CSS_SELECTOR, 'div#listing-detail')
        except Exception as e:
            listing_details = 'None'
            print('Listing details not found...')

        location_block_num = self.get_element_text(listing_details, By.CSS_SELECTOR, 'h1.listing-name')
        floor_size_psf = self.get_element_text(listing_details, By.CSS_SELECTOR, 'div#listing-Xvalue-size')
        price = self.get_element_text(listing_details, By.CSS_SELECTOR, 'div.listing-price div')
        num_bedroom = self.get_element_text(listing_details, By.CSS_SELECTOR, 'div.bed-box span.bed-numbers')
        num_bathroom = self.get_element_text(listing_details, By.CSS_SELECTOR, 'div.bath-box span.bath-numbers')
            
        # Description
        try:
            description = listing_details.find_element(By.CSS_SELECTOR, 'div.listing-description').text
        except Exception as e:
            description = ''

        # Agent details 
        try:
            agent_details= listing_details.find_element(By.CSS_SELECTOR, 'div.listing-agent-box')
        except Exception as e:
            agent_details = 'None'
            print('Agent detials section not found...')
        
        agent_name = self.get_attribute_value(agent_details, By.CSS_SELECTOR, 'div.agent-name span', 'innerText')
        agent_id = self.get_attribute_value(agent_details, By.CSS_SELECTOR, 'div.agent-image-cea div.agent-cea-reg', 'innerText')
        agent_phone_num = self.get_attribute_value(agent_details, By.CSS_SELECTOR, 'div a.featuredAgentCall', 'href')

        # Property details
        try:
            property_details_section = driver.find_element(By.CSS_SELECTOR, 'div.about-this-property')
            property_details = property_details_section.find_elements(By.CSS_SELECTOR, 'div.listing-about')
        except Exception as e:
            property_details = 'None'
            print('Properties details section not found...')


        if property_details != 'None':
            property_details_key = [self.get_element_text(item, By.CSS_SELECTOR, 'div.listing-about-main-key') for item in property_details]
            property_details_value = [self.get_element_text(item, By.CSS_SELECTOR, 'div.listing-about-main-value') for item in property_details]
        else:
            property_details_key = []
            property_details_value = []
        
        properties_details_dict = dict(zip(property_details_key, property_details_value))

        # Facilities
        try:
            facilities_section = driver.find_elements(By.CSS_SELECTOR, 'div.facilities-div div.facilities-row')
            facilities = [self.get_element_text(item, By.CSS_SELECTOR, 'span.listing-about-facility-span') for item in facilities_section]
            facilities = [facility for facility in facilities if facility != 'None']
        except Exception as e:
            facilities = []
            print('Facilities section not found...')

        # Nearby amenities
        try:
            nearby_amenities = driver.find_element(By.CSS_SELECTOR, 'div.nearby-amenities')
        except Exception as e:
            nearby_amenities = 'None'
            print('Nearby amenities section not found...')

        # Train stations
        try:
            train_station_element = nearby_amenities.find_elements(By.CSS_SELECTOR, 'div.Trains div.listing-amenity')
            train_stations = [f"{self.get_element_text(item, By.CSS_SELECTOR, 'div.listing-amenity-name')} "
                              f"{self.get_element_text(item, By.CSS_SELECTOR, 'div.listing-amenity-station span')}"
                              for item in train_station_element]
            train_stations = [station for station in train_stations if 'None' not in station]
        except Exception as e:
            train_stations = []
            print('Train station information not found...')

        # Schools
        try:
            school_element = nearby_amenities.find_elements(By.CSS_SELECTOR, 'div.Schools')
            schools = [nested_item.text for item in school_element for nested_item in item.find_elements(By.CSS_SELECTOR, 'div.listing-amenity-name')]
        except Exception as e:
            schools = []
            print('Schools information not found...')

        # Shopping Malls and Markets
        try:
            shopping_mall_element = nearby_amenities.find_element(By.CSS_SELECTOR, 'div.Shopping-Malls')
            shopping_malls = [item.text for item in shopping_mall_element.find_elements(By.CSS_SELECTOR, 'div.listing-amenity-name')]
        except Exception as e:
            shopping_malls = []
            print('Shopping mall information not found...')

        try:
            markets_element = nearby_amenities.find_element(By.CSS_SELECTOR, 'div.Markets')
            markets = [item.text for item in markets_element.find_elements(By.CSS_SELECTOR, 'div.listing-amenity-name')]
        except Exception as e:
            markets = []
            print('Market information not found...')

        # Save the scraped data
        scraped_data = {
            'url': driver.current_url,
            'location': location_block_num,
            'floor_size_psf': floor_size_psf,
            'price': price,
            'num_bedroom': num_bedroom,
            'num_bathroom': num_bathroom,
            'description': description,
            'agent_name': agent_name,
            'agent_id': agent_id,
            'agent_phone_num': agent_phone_num,
            'address': properties_details_dict.get('Address', 'None'),
            'property_name': properties_details_dict.get('Property Name', 'None'),
            'property_type': properties_details_dict.get('Property Type', 'None'),
            'model': properties_details_dict.get('Model', 'None'),
            'bedrooms': properties_details_dict.get('Bedrooms', 'None'),
            'bathrooms': properties_details_dict.get('Bathrooms', 'None'),
            'furnish': properties_details_dict.get('Furnish', 'None'),
            'floor_level': properties_details_dict.get('Floor Level', 'None'),
            'tenure': properties_details_dict.get('Tenure', 'None'),
            'developer': properties_details_dict.get('Developer', 'None'),
            'built_year': properties_details_dict.get('Built Year', 'None'),
            'hdb_town': properties_details_dict.get('HDB Town', 'None'),
            'asking': properties_details_dict.get('Asking', 'None'),
            'size': properties_details_dict.get('Size', 'None'),
            'psf': properties_details_dict.get('PSF', 'None'),
            'tenancy_status': properties_details_dict.get('Tenancy Status', 'None'),
            'date_listed': properties_details_dict.get('Date Listed', 'None'),
            'facilities': ', '.join(facilities),
            'train_stations': ', '.join(train_stations),
            'schools': ', '.join(schools),
            'shopping_mall/markets': ', '.join(shopping_malls + markets),
        }

        self.data.append(scraped_data)


    def scrape(self):
        for idx, url in enumerate(self.url_list):
            try:
                print(f'Scraping Property: {idx} URL:{url}')
                self.driver.get(url)
                self.scrolling_action(self.driver)
                self.parse_property_page(self.driver)
                self.scrolling_action(self.driver)
                if idx % 50 == 0:
                    time.sleep(10)  # Pause every 50 requests
            except Exception as e:
                print(f'Error on URL {url}: {e}')
        self.driver.quit()


    def save_data(self, filename):
        with open(filename, 'w') as json_file:
            json.dump(self.data, json_file, indent=4)

    def scrolling_action(self, driver):
        scrolling_times = random.randint(2, 4)
        scrolling_pixels = [2000, 3000]
        pixel = random.choice(scrolling_pixels)
        for i in range(scrolling_times):
            if i % 2 == 0:
                ActionChains(driver).scroll_by_amount(0, pixel).perform()
            else:
                ActionChains(driver).scroll_by_amount(0, -pixel).perform()
            time.sleep(2)

    def get_element_text(self, parent, by, value, default='None'):
        if parent == 'None':
            return 'None'
        try:
            return parent.find_element(by, value).text.strip()
        except NoSuchElementException:
            return default
    
    def get_attribute_value(self, parent, by, value, attribute, default='None'):
        if parent == 'None':
            return 'None'
        try:
            return parent.find_element(by, value).get_attribute(attribute).strip()
        except NoSuchElementException:
            return default


def load_url_list(csv_path):
    df = pd.read_csv(csv_path)
    return deduplicate_list(df['url'].tolist())

def deduplicate_list(input_list):
    return list(dict.fromkeys(input_list))

def srx_selenium(*op_args):
    driver_url = 'http://selenium-chrome:4444/wd/hub'
    options = webdriver.ChromeOptions()
    csv_path_to_urls = op_args[0]

    scraper = SrxSpider(driver_url, options, csv_path_to_urls)
    scraper.scrape()

    # Save the scraped data to a JSON file
    output_file_path = f'/opt/airflow/result/srx/{datetime.datetime.today().date()}_srx_properties.json'
    output_file = f'{datetime.datetime.today().date()}_srx_properties.json'
    scraper.save_data(output_file_path)
    return output_file

