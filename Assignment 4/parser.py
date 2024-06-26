import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["cs_websites"]
pages_collection = db["pages"]
professors_collection = db["professors"]

def parse_faculty_info(html):
    faculty_info = []
    soup = BeautifulSoup(html, 'html.parser')

    faculty_divs = soup.find_all(class_='clearfix')
    for faculty_div in faculty_divs:
        name_elem = faculty_div.find('h2')
        if name_elem:
            name = name_elem.text.strip() 
        else: 
            continue
        
        title_elem = faculty_div.find('strong', string=re.compile("Title"))
        title = title_elem.find_next_sibling('br').previous_sibling.strip() if title_elem else None
        
        office_elem = faculty_div.find('strong', string=re.compile("Office"))
        office = office_elem.find_next_sibling('br').previous_sibling.strip() if office_elem else None
        
        phone_elem = faculty_div.find('strong', string=re.compile("Phone"))
        phone = phone_elem.find_next_sibling('br').previous_sibling.strip() if phone_elem else None
        
        email_elem = faculty_div.find('a', href=lambda href: href and href.startswith('mailto:'))
        email = email_elem['href'].replace('mailto:', '') if email_elem else None
        
        website_elem = faculty_div.find('a', href=lambda href: href and 'http' in href)
        website = website_elem['href'] if website_elem else None

        faculty_info.append({
            "name": name,
            "title": title,
            "office": office,
            "phone": phone,
            "email": email,
            "website": website
        })

    return faculty_info

def persist_professors_data(html):
    faculty_info = parse_faculty_info(html)
    if faculty_info:
        for professor in faculty_info:
            professors_collection.insert_one(professor)
            print(f"Professor {professor['name']} data persisted.")
    else:
        print("No faculty information found.")

def main():
    # URL of the Permanent Faculty page
    target_page_url = "https://www.cpp.edu/sci/computer-science/faculty-and-staff/permanent-faculty.shtml"

    # Retrieve HTML content from the webpage
    response = requests.get(target_page_url)
    if response.status_code == 200:
        page_html = response.content

        # Parse and persist professors' data
        persist_professors_data(page_html)

        # Store the HTML content in MongoDB for future use
        pages_collection.insert_one({"url": target_page_url, "html": page_html})
        print("Page HTML data stored in MongoDB.")
    else:
        print("Failed to retrieve page HTML.")

if __name__ == "__main__":
    main()
