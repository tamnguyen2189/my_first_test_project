# install selenium and other resources for crawling data
!pip
install
selenium
# install other resources for doing crawling
!apt
install
chromium - chromedriver

import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
from random import randint
import json
import re

from selenium import webdriver

# Set driver for Chrome
options = webdriver.ChromeOptions()
options.add_argument(
    '-headless')  # since we run selenium on Google Colab so we don't want a chrome browser opens, so it will run in the background
options.add_argument('-no-sandbox')


def get_url(url):
    driver = webdriver.Chrome('chromedriver',
                              options=options)  # Define the chrome drivers with setting options we define above
    driver.get(url)  # Open the browser again to get web page
    html_data = driver.page_source  # After driver.get() is done, you can get back HTML string by using .page_source
    driver.close()  # Close the driver after retrieving the web page

    soup = BeautifulSoup(html_data, 'html.parser')

    return soup


def extract_tiki_info(url):
    """ Extract info from all products of a specfic category on Tiki website
        Input: url
        Output: info of products, saved as list of dictionary. If no products shown, return empty list.
    """
    data = []
    index = 1
    soup = get_url(url)

    # FIND ALL PRODUCT ITEMS
    products = soup.find_all('a', {'class': 'product-item'})
    all_script = soup.find_all('script', {'type': 'application/ld+json'})
    print("BATCH SIZE:", len(products))

    if (soup.find('div', {'class': 'style__StyledNotFoundProductView-sc-1uz0b49-0'})):
        print("END PAGE")
    elif len(products):
        # EXTRACT INFO TO DICTIONARY
        for i in products:
            d = {'name': '', 'price': '', 'product_url': '', 'image': '', 'product_sku': '',
                 'tiki_now': '', 'freeship': '', 'review': '', 'rating': '', 'under_price': '',
                 'discount': '', 'installment': '', 'gift': ''}

            try:
                d['name'] = i.find('div', {'class': 'name'}).text
                d['price'] = int(re.sub('[. ₫]', '', i.find('div', {'class': 'price-discount__price'}).text))
                d['product_url'] = 'https://tiki.vn' + i['href']
                thumbnail = i.find('div', {'class': 'thumbnail'})
                d['image'] = thumbnail.img['src']
                d['tiki_now'] = bool(i.find('div', {'class': 'badge-service'}).find('div', {'class': 'item'}))
                d['freeship'] = bool(i.find('div', {'class': 'badge-top'}).text == "Freeship")

                if i.find('div', {'class': 'review'}):
                    d['review'] = int(i.find('div', {'class': 'review'}).text.strip('(').strip(')'))
                else:
                    d['review'] = "N/A"

                d['under_price'] = bool(i.find('div', {'class': 'badge-under-price'}).find('div', {'class': 'item'}))

                if i.find('div', {'class': 'price-discount__discount'}):
                    d['discount'] = int(re.sub('[-%]', '', i.find('div', {'class': 'price-discount__discount'}).text))
                else:
                    d['discount'] = "N/A"

                d['installment'] = bool(i.find('div', {'class': 'badge-benefits'}).img)
                d['gift'] = bool(i.find('div', {'class': 'freegift-list'}))

                script = all_script[index]
                dict_content = json.loads(script.text)
                d['product_sku'] = dict_content['sku']

                if 'aggregateRating' in dict_content:
                    d['rating'] = float(dict_content['aggregateRating']['ratingValue'])
                else:
                    d['rating'] = "N/A"

            except Exception as e:
                print(e)

            index += 1
            data.append(d)

    return data


def scrape_tiki(base_url):
    result = []
    page_number = 1
    main, opt = base_url.split('?')

    while True:
        page = f'?page={page_number}&'
        url = main + page + opt
        print("url =", url)
        data = extract_tiki_info(url)
        if len(data) > 0:
            result.extend(data)
            if page_number == 3:
                break
        else:
            break
        page_number += 1
        sleep(randint(1, 2))

    print("****TOTAL = ", len(result))
    return result


import sqlite3
from google.colab import drive

drive.mount('/content/gdrive')
PATH_TO_DB = '/content/gdrive/MyDrive/'
conn = sqlite3.connect(PATH_TO_DB + 'tiki_1.db')
cur = conn.cursor()


# Create table categories in the database using a function
def create_categories_test_table():
    query = """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255),
            url TEXT, 
            parent_id INTEGER, 
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)


# create categories table
create_categories_table()
conn.commit()


# Create table products in the database using a function
def create_products_table():
    query = """
        CREATE TABLE IF NOT EXISTS products2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255),
            price INTEGER,
            product_url TEXT,
            image TEXT,
            product_sku INTEGER,
            tiki_now TEXT,
            freeship TEXT,
            review INTEGER,
            rating INTEGER,
            under_price TEXT,
            discount INTEGER,
            installment TEXT,
            gift TEXT,
            category_name TEXT,
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)


# create products table
create_products_table()
conn.commit()


def select_query(query, conn=conn):
    return pd.read_sql_query(query, conn)


# Instead of using a function to do CRUD on database,
# creating a class Category is preferred
# attributes: name, url, parent_id
# instance method: save_into_db()
class Category:
    def __init__(self, name, url, parent_id=None, cat_id=None):
        self.cat_id = cat_id
        self.name = name
        self.url = url
        self.parent_id = parent_id

    def __repr__(self):
        return f"ID: {self.cat_id}, Name: {self.name}, URL: {self.url}, Parent: {self.parent_id}"

    def save_into_db(self):
        query = """
            INSERT INTO categories (name, url, parent_id)
            VALUES (?, ?, ?);
        """
        val = (self.name, self.url, self.parent_id)
        try:
            cur.execute(query, val)
            self.cat_id = cur.lastrowid
            conn.commit()
        except Exception as err:
            print('ERROR BY INSERT:', err)


class Product:
    def __init__(self, name, price, product_url, image, product_sku, tiki_now, freeship, review, rating, under_price,
                 discount, installment, gift, category_name):
        self.name = name
        self.price = price
        self.product_url = product_url
        self.image = image
        self.product_sku = product_sku
        self.tiki_now = tiki_now
        self.freeship = freeship
        self.review = review
        self.rating = rating
        self.under_price = under_price
        self.discount = discount
        self.installment = installment
        self.gift = gift
        self.category_name = category_name

    def __repr__(self):
        return f"""

                Name: {self.name}, 
                Price: {self.price}, 
                Product_URL: {self.product_url},
                Image: {self.image}, 
                ID: {self.product_sku}, 
                Tiki_now: {self.tiki_now}
                Freeship: {self.freeship}, 
                Review: {self.review}, 
                Rating: {self.rating}
                Under_price: {self.under_price}
                Discount: {self.discount}, 
                Installment: {self.installment}, 
                Gift: {self.gift},
                Category: {self.category_name}
                """

    def save_into_product_db(self):
        query = """
            INSERT INTO products2 (name, price, product_url, image, product_sku, tiki_now, freeship, review, rating, under_price, discount, installment, gift, category_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        val = (self.name,
               self.price,
               self.product_url,
               self.image,
               self.product_sku,
               self.tiki_now,
               self.freeship,
               self.review,
               self.rating,
               self.under_price,
               self.discount,
               self.installment,
               self.gift,
               self.category_name
               )
        try:
            cur.execute(query, val)
            self.product_sku = cur.lastrowid
            conn.commit()
        except Exception as err:
            print('ERROR BY INSERT:', err)


# empty our category set
CATEGORY_SET = set()


def can_add_to_cat_set(cat_name, save=False):
    if cat_name not in CATEGORY_SET:
        if save:
            CATEGORY_SET.add(cat_name)
            print(f'Added "{cat_name}" to CATEGORY_SET')
        return True
    return False


def get_main_category(main_categories, save_db=False):
    result = []
    for i in main_categories:
        _ = can_add_to_cat_set(i['Name'], save_db)

        main_cat = Category(i['Name'], i['URL'])
        if save_db:
            main_cat.save_into_db()
        result.append(main_cat)
    return result


main_categories = [{'Name': 'Điện Tử - Điện Lạnh',
                    'URL': 'https://tiki.vn/tivi-thiet-bi-nghe-nhin/c4221?src=c.4221.hamburger_menu_fly_out_banner'},
                   {'Name': 'Phụ Kiện - Thiết Bị Số',
                    'URL': 'https://tiki.vn/thiet-bi-kts-phu-kien-so/c1815?src=c.1815.hamburger_menu_fly_out_banner'},
                   {'Name': 'Laptop - Thiết bị IT',
                    'URL': 'https://tiki.vn/laptop-may-vi-tinh/c1846?src=c.1846.hamburger_menu_fly_out_banner'},
                   {'Name': 'Máy Ảnh - Quay Phim',
                    'URL': 'https://tiki.vn/may-anh/c1801?src=c.1801.hamburger_menu_fly_out_banner'},
                   {'Name': 'Điện Gia Dụng',
                    'URL': 'https://tiki.vn/dien-gia-dung/c1882?src=c.1882.hamburger_menu_fly_out_banner'},
                   {'Name': 'Nhà Cửa Đời Sống',
                    'URL': 'https://tiki.vn/nha-cua-doi-song/c1883?src=c.1883.hamburger_menu_fly_out_banner'},
                   {'Name': 'Hàng Tiêu Dùng - Thực Phẩm',
                    'URL': 'https://tiki.vn/bach-hoa-online/c4384?src=c.4384.hamburger_menu_fly_out_banner'},
                   {'Name': 'Đồ chơi, Mẹ & Bé',
                    'URL': 'https://tiki.vn/me-va-be/c2549?src=c.2549.hamburger_menu_fly_out_banner'},
                   {'Name': 'Làm Đẹp - Sức Khỏe',
                    'URL': 'https://tiki.vn/lam-dep-suc-khoe/c1520?src=c.1520.hamburger_menu_fly_out_banner'},
                   {'Name': 'Thể Thao - Dã Ngoại',
                    'URL': 'https://tiki.vn/the-thao/c1975?src=c.1975.hamburger_menu_fly_out_banner'},
                   {'Name': 'Xe Máy, Ô tô, Xe Đạp',
                    'URL': 'https://tiki.vn/o-to-xe-may-xe-dap/c8594?src=c.8594.hamburger_menu_fly_out_banner'},
                   {'Name': 'Hàng quốc tế',
                    'URL': 'https://tiki.vn/hang-quoc-te/c17166?src=c.17166.hamburger_menu_fly_out_banner'},
                   {'Name': 'Sách, VPP & Quà Tặng',
                    'URL': 'https://tiki.vn/nha-sach-tiki/c8322?src=c.8322.hamburger_menu_fly_out_banner'},
                   {'Name': 'Voucher - Dịch Vụ - Thẻ Cào',
                    'URL': 'https://tiki.vn/voucher-dich-vu/c11312?src=c.11312.hamburger_menu_fly_out_banner'}
                   {'Name': 'Laptop - Thiết bị IT',
                    'URL': 'https://tiki.vn/laptop-may-vi-tinh/c1846?src=c.1846.hamburger_menu_fly_out_banner'}]

main_cat_objs = get_main_category(main_categories, save_db=True)

main_cat_objs

import re


# get_sub_categories() given a parent category
def get_sub_categories(parent_category, save_db=False):
    parent_url = parent_category.url
    result = []

    try:
        soup = get_url(parent_url)
        for a in soup.find_all('a', {'class': 'item item--category '}):
            name = a.text.strip()
            if can_add_to_cat_set(name, save_db):
                sub_url = a['href']
                cat = Category(name, sub_url,
                               parent_category.cat_id)  # we now have parent_id, which is cat_id of parent category
                if save_db:
                    cat.save_into_db()
                result.append(cat)
    except Exception as err:
        print('ERROR IN GETTING SUB CATEGORIES:', err)
    return result


# Recursive
def get_all_categories(categories, save_db):
    categories = categories.copy()
    while len(categories):
        cat_to_crawl = categories[0]
        print(f'Getting {cat_to_crawl} sub-categories...')
        sub_categories = get_sub_categories(cat_to_crawl, save_db=save_db)
        print(f'Finished! {cat_to_crawl.name} has {len(sub_categories)} sub-categories')
        categories += sub_categories
        del categories[0]


# turn main cat obj to a list
get_all_categories(main_cat_objs, save_db=True)

# empty our products set
PRODUCT_SET = set()


def can_add_to_product_set(product_name, save=False):
    if product_name not in PRODUCT_SET:
        if save:
            PRODUCT_SET.add(product_name)
            print(f'Added "{product_name}" to PRODUCT_SET')
        return True
    return False


def get_products(sub_url, category_name):
    try:
        result_all_pages = scrape_tiki(sub_url)
        for result in result_all_pages:
            _ = can_add_to_product_set(result['name'], save=True)
            product = Product(result['name'], result['price'], result['product_url'], result['image'],
                              result['product_sku'], result['tiki_now'], result['freeship'], result['review'],
                              result['rating'], result['under_price'], result['discount'], result['installment'],
                              result['gift'], category_name)
            product.save_into_product_db()
    except:
        print("get products error")


def select_query(query, conn=conn):
    return pd.read_sql_query(query, conn)


# Convert SQL table to list in Python: get the lowest_subcat_url
con = sqlite3.connect(PATH_TO_DB + 'tiki_1.db')
cur = conn.cursor()
cur.execute('SELECT * FROM categories WHERE id NOT IN (parent_id) ORDER BY id ASC')
result = cur.fetchall()
final = [list(i) for i in result]
final

# Get the products from Tiki
for element in final:
    get_products(element[2], element[1])

# Join two table of products
select_query(''' 
SELECT * FROM products1
UNION ALL
SELECT * FROM products2
ORDER BY category_name
''')

# what is the most expensive item for each categories?
select_query('''
    WITH total_products AS
        (
        SELECT * FROM products1
        UNION ALL
        SELECT * FROM products2
        ORDER BY category_name 
        )
    SELECT name, MAX(price), category_name, review, rating FROM total_products
    GROUP BY category_name

''')

# Which catogory contains the most items?
select_query('''
    WITH total_products AS
        (
        SELECT * FROM products1
        UNION ALL
        SELECT * FROM products2
        ORDER BY category_name 
        )
    SELECT COUNT(id), category_name FROM total_products
    GROUP BY category_name
    ORDER BY COUNT(id) DESC LIMIT 10

''')

# Which category/product has the highest reviews or ratings ...?
select_query('''
    WITH total_products AS
        (
        SELECT * FROM products1
        UNION ALL
        SELECT * FROM products2
        ORDER BY category_name 
        )
    SELECT review, category_name FROM total_products
    WHERE review NOT IN ('N/A')
    GROUP BY category_name
    ORDER BY review DESC LIMIT 10

''')

# Which category/product has the highest reviews or ratings ...?
select_query('''
    WITH total_products AS
        (
        SELECT * FROM products1
        UNION ALL
        SELECT * FROM products2
        ORDER BY category_name 
        )
    SELECT rating, category_name FROM total_products
    WHERE rating NOT IN ('N/A')
    GROUP BY category_name
    ORDER BY rating DESC LIMIT 10

''')