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

class PropnexSpider:
    def __init__(self, driver_url, options, csv_path_to_urls):
        
        self.driver = webdriver.Remote(command_executor=driver_url, options=options)
        self.url_list = load_url_list(csv_path_to_urls)
        self.data = [] 

    def parse_property_page(self, driver):
        try:
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div.col-lg-4')))
            details_section = driver.find_element(By.CSS_SELECTOR, 'div.col-lg-4')
        except Exception as e:
            details_section = 'None'
            print('Details section not found...')

        location = self.get_element_text(details_section, By.TAG_NAME, 'h1')
        price = self.get_element_text(details_section, By.CSS_SELECTOR, 'strong.mr-1')
        price_psf = self.get_element_text(details_section, By.CSS_SELECTOR, 'p.mt-2')

        try:
            li_elements = details_section.find_elements(By.CSS_SELECTOR, 'div.lbb-21 li')
        except Exception as e:
            li_elements = 'None'
            print('Details section information not found...')

        street_town_district, num_bedroom, num_bathroom, floor_area_sqft = 'None', 'None', 'None', 'None'
        if li_elements != 'None':
            for li in li_elements:
                try:
                    img = li.find_element(By.TAG_NAME, 'img')
                    src = img.get_attribute('src')
                    if 'ic_location.png' in src:
                        street_town_district = li.find_element(By.TAG_NAME, 'p').text.strip()
                    elif 'ic_beds.png' in src:
                        num_bedroom = li.text.strip()
                    elif 'ic_baths.png' in src:
                        num_bathroom = li.text.strip()
                    elif 'ic_sqft.png' in src:
                        floor_area_sqft = li.text.strip()
                    else:
                        continue
                except Exception as e:
                    continue    

        # Agent information
        try:
            agent_details = details_section.find_element(By.CSS_SELECTOR, 'div.agent-dt-box')
        except Exception as e:
            agent_details = 'None'
            print('Agent details section not found...')

        agent_name = self.get_element_text(agent_details, By.TAG_NAME, 'h5')

        try:
            agent_details_p_tags = agent_details.find_elements(By.TAG_NAME, 'p')
        except Exception as e:
            agent_details_p_tags = 'None'
            print('Agent details p tages not found...')
        
        agent_id, agent_email, agent_phone_number = 'None', 'None', 'None'
        if agent_details_p_tags != 'None':
            for item in agent_details_p_tags:
                if item.text.strip().startswith('#R'):
                    agent_id = item.text.strip()
                elif '+65' in item.text:
                    agent_phone_number = item.text.strip()
                elif '.com' in item.text.lower():
                    agent_email = item.text.strip()
                else:
                    continue
        
        # Properties details
        details_names = []
        details_value = []
        try:
            properties_details_section = driver.find_elements(By.CSS_SELECTOR, 'div.property-list-box ul')
        except Exception as e:
            properties_details_section = 'None'
            print('Properties details section not found...')
        if properties_details_section != 'None':
            for ind, item in enumerate(properties_details_section):
                if ind % 2 == 0:
                    details_names.extend([li.text.strip() for li in item.find_elements(By.TAG_NAME, 'li')])
                else:
                    details_value.extend([li.text.strip() for li in item.find_elements(By.TAG_NAME, 'li')])

        properties_details = dict(zip(details_names, details_value))

        # Press the show more button in the description section
        try:
            description_show_more_button = driver.find_element(By.ID, 'rm-more_1')
            
            driver.execute_script('arguments[0].scrollIntoView(true);', description_show_more_button)
            driver.execute_script('arguments[0].click();', description_show_more_button)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.desc-box p'))  
            )
        except Exception as e:
            print('Show more button in the description section not found...')
        
        # Press the show more button in the facility section
        try:
            facility_show_more_button = driver.find_element(By.ID, 'showMoreFacility')
            
            driver.execute_script('arguments[0].scrollIntoView(true);', facility_show_more_button)
            driver.execute_script('arguments[0].click();', facility_show_more_button)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'li.facilities-icons'))
            )
        except Exception as e:
            print('Show more button in the facility section not found...')

        # Description
        try:
            description = driver.find_element(By.CSS_SELECTOR, 'div.desc-box p').text
        except Exception as e:
            description = ''

        # Facilities
        try:
            facilities = driver.find_elements(By.CSS_SELECTOR, 'li.facilities-icons')
            facilities_list = [item.find_element(By.TAG_NAME, 'p').text for item in facilities]
        except Exception as e:
            facilities_list = []

        # Store data in dictionary
        scraped_data = {
            'url': driver.current_url,
            'location': location,
            'price': price,
            'price_psf': price_psf,
            'street_town_district': street_town_district,
            'num_bedroom': num_bedroom,
            'num_bathroom': num_bathroom,
            'floor_area_sqft': floor_area_sqft,
            'agent_name': agent_name,
            'agent_id': agent_id,
            'agent_email': agent_email,
            'agent_phone_num': agent_phone_number,
            'listing_type': properties_details.get('Listing Type', 'None'),
            'property_group': properties_details.get('Property Group', 'None'),
            'property_type': properties_details.get('Property Type', 'None'),
            'district': properties_details.get('District', 'None'),
            'total_floor_area': properties_details.get('Floor Area', 'None'),
            'top': properties_details.get('TOP', 'None'),
            'furnishing': properties_details.get('Furnishing', 'None'),
            'tenure': properties_details.get('Tenure', 'None'),
            'floor': properties_details.get('Floor', 'None'),
            'post_code': properties_details.get('Post Code', 'None'),
            'street_name': properties_details.get('Street Name', 'None'),
            'description': description,
            'facilities': ', '.join(facilities_list)
        }
        self.data.append(scraped_data)
        

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


    def get_element_text(self, parent, by, value, default='None'):
        if parent == 'None':
            return 'None'
        try:
            return parent.find_element(by, value).text.strip()
        except NoSuchElementException:
            return default
        

def load_url_list(csv_path):
    df = pd.read_csv(csv_path)
    return deduplicate_list(df['url'].tolist())

def deduplicate_list(input_list):
    return list(dict.fromkeys(input_list))

def propnex_selenium(*op_args):
    driver_url = 'http://selenium-chrome:4444/wd/hub'
    options = webdriver.ChromeOptions()
    csv_path_to_urls = op_args[0] 

    scraper = PropnexSpider(driver_url, options, csv_path_to_urls)
    scraper.scrape()

    # Save the scraped data to a JSON file
    output_file_path = f'/opt/airflow/result/propnex/{datetime.datetime.today().date()}_propnex_properties.json'
    output_file = f'{datetime.datetime.today().date()}_propnex_properties.json'
    scraper.save_data(output_file_path)
    return output_file
