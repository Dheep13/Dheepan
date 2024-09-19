import requests
from bs4 import BeautifulSoup
import argparse

def get_amazon_data(product):
    url = f'https://www.amazon.com/s?k={product}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Extract price from the webpage
    price_element = soup.find('span', {'class': 'a-offscreen'})
    price = price_element.text.strip() if price_element else 'N/A'
    # Other data extraction code goes here
    amazon_data = {
        'price': price,
        'delivery_date': '',
        'delivery_charges': '',
        'terms': ''
    }
    print (amazon_data)
    return amazon_data


def get_flipkart_data(product):
    url = f'https://www.flipkart.com/search?q={product}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Extract price from the webpage
    price_element = soup.find('div', {'class': '_30jeq3 _1_WHN1'})
    price = price_element.text.strip() if price_element else 'N/A'
    # Other data extraction code goes here
    flipkart_data = {
        'price': price,
        'delivery_date': '',
        'delivery_charges': '',
        'terms': ''
    }
    print (flipkart_data)
    return flipkart_data


def compare_data(amazon_data, flipkart_data):
    report = ""
    report += "Comparison Report:\n\n"

    # Compare prices
    amazon_price = amazon_data.get('price')
    flipkart_price = flipkart_data.get('price')
    if amazon_price and flipkart_price:
        report += "Price:\n"
        report += f"Amazon: ${amazon_price}\n"
        report += f"Flipkart: ₹{flipkart_price}\n"
        report += "\n"

    # Compare delivery dates
    amazon_delivery_date = amazon_data.get('delivery_date')
    flipkart_delivery_date = flipkart_data.get('delivery_date')
    if amazon_delivery_date and flipkart_delivery_date:
        report += "Delivery Date:\n"
        report += f"Amazon: {amazon_delivery_date}\n"
        report += f"Flipkart: {flipkart_delivery_date}\n"
        report += "\n"

    # Compare delivery charges
    amazon_delivery_charges = amazon_data.get('delivery_charges')
    flipkart_delivery_charges = flipkart_data.get('delivery_charges')
    if amazon_delivery_charges and flipkart_delivery_charges:
        report += "Delivery Charges:\n"
        report += f"Amazon: ${amazon_delivery_charges}\n"
        report += f"Flipkart: ₹{flipkart_delivery_charges}\n"
        report += "\n"

    # Compare terms and conditions
    amazon_terms = amazon_data.get('terms')
    flipkart_terms = flipkart_data.get('terms')
    if amazon_terms and flipkart_terms:
        report += "Terms & Conditions:\n"
        report += "Amazon:\n"
        report += f"{amazon_terms}\n"
        report += "Flipkart:\n"
        report += f"{flipkart_terms}\n"
        report += "\n"

    return report

def save_report(report):
    with open('comparison_report.txt', 'w') as file:
        file.write(report)
    print('Comparison report saved successfully.')

def main(product):
    # Call the functions to get data from Amazon and Flipkart
    amazon_data = get_amazon_data(product)
    print(amazon_data)
    flipkart_data = get_flipkart_data(product)
    print(amazon_data)
    # Compare the data and generate the report
    comparison_report = compare_data(amazon_data, flipkart_data)
    # Save the report to a file
    save_report(comparison_report)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Web scraping comparison tool for Amazon and Flipkart.')
    parser.add_argument('product', type=str, help='Product name and model to search')
    args = parser.parse_args()
    main(args.product)
