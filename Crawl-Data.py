import requests
from bs4 import BeautifulSoup
import pyodbc
import requests
from datetime import datetime, timedelta

# Hàm để cào dữ liệu từ một trang web dựa trên ngày tháng năm
def scrape_data(start_date, end_date):
    base_url = 'https://www.minhngoc.net.vn/ket-qua-xo-so/'
    date_format = '%d-%m-%Y'
    current_date = start_date

    while current_date <= end_date:
        # Tạo URL từ ngày hiện tại
        url = base_url + current_date.strftime(date_format) + '.html'

        print(url)

        # Kết nối với SQL Server
        con = pyodbc.connect('Driver={SQL Server};'
                     'Server=DESKTOP-AL8SAM5;'
                     'Database=XoSoMienNam;'
                     'Trusted_Connection=yes;')

        cursor = con.cursor()

        # Truy cập vào đường link trang web sổ xố minh ngọc
        response = requests.get(url)

        # Nội dung trang web
        soup = BeautifulSoup(response.content, 'html.parser')

        # Khởi tạo biến chứa bảng kết quả sổ xố miền nam
        bkqmn = soup.find('table', class_='bkqmiennam')

        # Khởi tạo mảng chứa giá trị tạm thời
        values_temp = []

        # Khởi tạo biến chứa kết quả sổ xố của các tỉnh trong ngày
        provinces = bkqmn.find_all('table', class_='rightcl')

        # Tạo mảng chứa các class cần duyệt
        prizes = ['giaidb', 'giai1', 'giai2', 'giai3', 'giai4', 'giai5', 'giai6', 'giai7', 'giai8']

        for pro in provinces:

          sample = {'Ngay': None,
                  'Tinh': None,
                  'GiaiDB': None,
                  'Giai1': None,
                  'Giai2': None,
                  'Giai3_1': None,
                  'Giai3_2': None,
                  'Giai4_1': None,
                  'Giai4_2': None,
                  'Giai4_3': None,
                  'Giai4_4': None,
                  'Giai4_5': None,
                  'Giai4_6': None,
                  'Giai4_7': None,
                  'Giai5': None,
                  'Giai6_1': None,
                  'Giai6_2': None,
                  'Giai6_3': None,
                  'Giai7': None,
                  'Giai8': None}

          # Khởi tạo biến chứa Ngày sổ xố
          date = bkqmn.find('td', class_='ngay').text.strip()
          values_temp.append(date)

          province = pro.find('td', class_='tinh').text
          values_temp.append(province)

          for pri in prizes:
            numbers = pro.find('td', class_=pri).find_all('div')
            for number in numbers:
              values_temp.append(number.text)

          for index, key in enumerate(sample.keys()):
            sample[key] = values_temp[index]

          try:
            cursor.execute('INSERT INTO KetQuaSoXo VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                          sample['Ngay'], sample['Tinh'], sample['GiaiDB'], sample['Giai1'], sample['Giai2'],
                          sample['Giai3_1'], sample['Giai3_2'], sample['Giai4_1'], sample['Giai4_2'], sample['Giai4_3'],
                          sample['Giai4_4'], sample['Giai4_5'], sample['Giai4_6'], sample['Giai4_7'], sample['Giai5'],
                          sample['Giai6_1'], sample['Giai6_2'], sample['Giai6_3'], sample['Giai7'], sample['Giai8'])

            con.commit()
          except Exception as e:
            print("Error occurred while executing SQL query:", e)

          values_temp.clear()

        print('Insert Complete in ' + date)

        # Tăng ngày hiện tại lên 1
        current_date += timedelta(days=1)

# Ngày bắt đầu và kết thúc muốn cào dữ liệu
start_date = datetime(2008, 1, 1)
end_date = datetime.now()

# Gọi hàm cào dữ liệu với khoảng thời gian muốn cào
scrape_data(start_date, end_date)