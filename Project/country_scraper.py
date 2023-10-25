import requests
from bs4 import BeautifulSoup
import json

# Define the URL of the website you want to scrape
url = 'https://www.globalis.dk/Lande'

# Send an HTTP GET request to the URL
response = requests.get(url)

# Check for a valid response
if response.status_code == 200:
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Locate the <main> element
    main_element = soup.find('main')
    
    if main_element:
        # Locate all <a> elements with class="list-links__link" within the <main> element
        a_elements = main_element.find_all('a', class_='list-links__link')
        
        if a_elements:
            # Initialize an empty list to store the extracted text
            extracted_texts = []
            
            for a_element in a_elements:
                # Extract the text contained within the <a> element
                text = a_element.text
                extracted_texts.append(text)
                    
            # Output the resulting list to a JSON file
            with open('..\countries.json', 'w', encoding='utf-8') as file:
                json.dump(extracted_texts, file, ensure_ascii=False, indent=4)
            
        else:
            print("No <a> elements with class='list-links__link' found within the <main> element")
    else:
        print("No <main> element found")
else:
    print(f"Failed to retrieve the page: {response.status_code}")
