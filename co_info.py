import pandas as pd
from glob import glob
import concurrent.futures
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import dart_fss as dart
import FinanceDataReader as fdr
import conn_db
import helper

user_agent = helper.user_agent
to_path = conn_db.get_path('stock')
#--------------------------------------------------------------------------------------------------------------------
def update_co_by_theme_from_naver():  # 네이버 증권에 있는 테마별 회사정보 가져오기
    '''
    네이버 증권에 있는 테마별 회사정보 가져오기
    '''
    #append 시킬 비어있는 df 생성
    theme_list = pd.DataFrame()
    #네이버 테마 페이지가 6페이지까지 있음. 1페이지씩 돌면서 정보 가져오기
    for page in range(1,7):
        helper.random_sleep()
        url = "https://finance.naver.com/sise/theme.nhn?&page={}".format(page)  #loop에서 page 받기
        response = requests.get(url, headers = {'User-Agent': user_agent})
        dom = BeautifulSoup(response.text, "html.parser")
        elements = dom.select("#contentarea_left > table.type_1.theme > tr")
        # 빈 리스트 생성
        temp = []
        for element in elements[3:-2]:
            try:
                data = {
                    "theme": element.select("td")[0].text,
                    "link": "https://finance.naver.com/" + element.select("td")[0].select_one("a").get("href")
                }
            except:
                pass
            temp.append(data) #비어있는 리스트에 채우기
        #회별 loop 완료후 theme_list에 채우기
        theme_list = theme_list.append(temp, ignore_index=True)
    #------------------------------------------------------------------------------
    co_by_theme = pd.DataFrame() #append 시킬 비어있는 df 생성
    for theme, link in theme_list.values:
        response = requests.get(link, headers = {'User-Agent': user_agent})
        parse = BeautifulSoup(response.text, "html.parser")
        co_list = parse.find_all('div',{'class': 'name_area'}) # 종목명과 종목 코드 추출할 리스트
        co_info = parse.find_all('p',{'class': 'info_txt'}) # 종목설명 추출할 리스트
        #------------------------------------------------------------------------------
        temp_name = [item.text.split('*')[0] for item in co_list] #종목별 회사명 추출
        temp_code = [item.select_one('a').get('href').split('=')[1] for item in co_list] #종목별 종목코드 추출
        #------------------------------------------------------------------------------
        #종목별 설명 추출
        temp_info = [info.text.split('*')[0] for info in co_info]
        temp_theme_info = temp_info[0] # 0번있는 테마 설명 저장
        temp_co_info = temp_info[1:] # 0번에는 테마 설명은 제외하고 그 다음부터 있는 종목 설명 저장
        #------------------------------------------------------------------------------
        #df로 만들기
        temp_df = pd.DataFrame(temp_name) #종목명 추가
        temp_df['종목코드'] = pd.DataFrame(temp_code) #종목코드 추가
        temp_df['종목설명'] = pd.DataFrame(temp_co_info) #종목설명 추가
        temp_df['테마설명'] = str(temp_theme_info) #테마설명 추가
        temp_df['테마명'] = str(theme)  #테마명 추가
        #------------------------------------------------------------------------------
        #최종 df에 추가
        co_by_theme = co_by_theme.append(temp_df, ignore_index=False)
    # 컬럼명 0으로 되어 있는 '종목명'은 삭제. KRX와 종목코드로 mapping해서 거기 있는 종목명 사용
    co_by_theme = co_by_theme.drop(columns=[0]).reset_index(drop=True)

    # KRX df 불러와서 KEY 컬럼 만들기
    df_map_code = conn_db.from_('DB_기업정보', 'from_krx')[['종목코드', 'KEY','종목명']]
    co_by_theme = co_by_theme.merge(df_map_code, on='종목코드')

    # 오류수정 ---------------------------------------------------------
    co_by_theme.loc[co_by_theme['종목코드']=='091340', '테마명'] = 'OLED(유기 발광 다이오드)'
    # 오류수정 ---------------------------------------------------------
    # 파일 저장
    # co_by_theme.to_pickle(path + r'취합완료_상장사기업정보\테마별종목_naver_final.pkl') # 파일 저장 생략
    conn_db.to_(co_by_theme, 'DB_기업정보', 'from_naver_theme')
    # 회사별 테마 한줄 짜리 df 만들어서 저장
    co_by_theme.drop(columns=['테마설명'], inplace=True)

    cols = ['종목명', '종목코드','KEY']
    co_by_theme = co_by_theme.groupby(cols)['테마명'].apply(', '.join).reset_index()
    conn_db.to_(co_by_theme, 'DB_기업정보', 'from_naver_theme2')
    print('네이버증권 테마 업데이트 완료')

def update_co_by_industry_from_naver():  # 네이버 증권에 있는 회사별 업종정보 가져오기
    '''
    네이버 증권에 있는 회사별 업종정보 가져오기
    '''
    url = "https://finance.naver.com/sise/sise_group.nhn?type=upjong"
    r = requests.get(url, headers = {'User-Agent': user_agent})
    dom = BeautifulSoup(r.text, "html.parser")
    elements = dom.select("#contentarea_left > table > tr > td > a")
    #------------------------------------------------------------------------------
    industry_list = pd.DataFrame()
    industry_list['업종명'] = [item.text for item in elements]
    industry_list['link'] = [item.get('href') for item in elements]
    industry_list['link'] = "https://finance.naver.com" + industry_list['link'].astype(str)
    #------------------------------------------------------------------------------
    co_by_industry = pd.DataFrame()
    for industry, link in industry_list.values:
        helper.random_sleep()
        r = requests.get(link, headers = {'User-Agent': user_agent})
        parse = BeautifulSoup(r.text, "html.parser")
        items = parse.find_all('div',{'class': 'name_area'})
        #------------------------------------------------------------------------------------------
        temp_df = pd.DataFrame()
        temp_df['종목명'] = [item.text.split('*')[0].rstrip() for item in items]
        temp_df['종목코드'] = [item.select_one('a').get('href').split('=')[-1] for item in items]
        temp_df['industry'] = str(industry)
        co_by_industry = co_by_industry.append(temp_df, ignore_index=True)
    #------------------------------------------------------------------------------
    # '종목명'은 삭제. KRX와 종목코드로 mapping해서 거기 있는 종목명 사용
    co_by_industry.drop(columns=['종목명'], inplace=True)
    co_by_industry = co_by_industry.drop_duplicates().reset_index(drop=True)
    cols = ['종목코드', 'KEY','종목명']
    df_map_code = conn_db.from_('DB_기업정보', 'from_krx')[cols]
    co_by_industry = co_by_industry.merge(df_map_code, on='종목코드')
    conn_db.to_(co_by_industry, 'DB_기업정보', 'from_naver_industry')
    print('네이버증권 업종 업데이트 완료')

def update_naver_industry_per(): # 네이버 업종별 PER 업데이트
    print('네이버 업종별 PER 가져오기 시작')
    cols = ['종목코드','industry']
    industry_df = conn_db.from_('DB_기업정보','from_naver_industry')[cols]
    industry_df = industry_df.groupby(['industry'],as_index='False').head(1)
    filt = industry_df['industry']!='기타'
    industry_df = industry_df.loc[filt].copy()
    #---------------------------------------------------------
    for code in industry_df['종목코드']:
        url = f'https://finance.naver.com/item/main.nhn?code={code}'
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.content, "lxml")
        industry_per = dom.select('#tab_con1 > div > table > tr > td > em')[-2].text
        industry_df.loc[industry_df['종목코드']==code,'업종PER'] = industry_per
    industry_df = industry_df.rename(columns={'industry':'업종_naver'})
    industry_df = industry_df.drop(columns='종목코드').reset_index(drop=True)

    filt = industry_df['업종PER'].str.contains(',')
    industry_df.loc[filt, '업종PER'] = industry_df.loc[filt, '업종PER'].str.replace(',','')
    industry_df['업종PER'] = pd.to_numeric(industry_df['업종PER'], errors='coerce')

    conn_db.to_(industry_df,'DB_기업정보','네이버업종PER')
    print('네이버 업종별 PER 가져오기 완료')

def clean_new_high_low(): # KRX에서 받는 신고가신저가 전처리
    folder = glob(r"C:\Users\bong2\Downloads\\" + '*.csv')
    df = pd.concat([pd.read_csv(file, encoding='utf-8') for file in folder ])

    cols = ['현재가','대비','등락률','종목명','종료일 종가','거래량']
    df.drop(columns=cols, inplace=True)
    df.rename(columns={'가격(원)':'신고가', '일자':'신고가 날짜'}, inplace=True)

    df = helper.make_keycode(df)
    code_list = conn_db.from_('DB_기업정보','FS_update_list')['KEY']
    df = df.loc[df['KEY'].isin(code_list)].copy()

    df['신고가 날짜'] = df['신고가 날짜'].str.replace('/','-')
    df['신고가'] = df['신고가'].apply(lambda x : x.replace(',','') if ',' in x else x)
    df['신고가'] = df['신고가'].astype(int)

    file = to_path+'신고가신저가.pkl'
    df = df.concat([df,pd.read_pickle(file)]).drop_duplicates().reset_index(drop=True)
    conn_db.export_(df, to_path+'신고가신저가')
    df.drop(columns=['종목코드','종목명'], inplace=True)
    conn_db.to_(df, 'DB_기업정보','신고가신저가')
    del df, code_list
    #--------------------------------------------------------------------------------------------------------------------
def update_company_explain_from_naver(param='all'):  # 네이버증권에서 종목 설명 정보 전처리
    print('네이버에서 기업설명 가져오기 시작')
    start_time = helper.now_time()
    code_list = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
    #--------------------------------------------------------------------------------------------
    def get_company_explain_from_naver(code): # 네이버증권에서 종목 설명 정보 가져오기
        # helper.random_sleep()
        url = f'https://finance.naver.com/item/main.nhn?code={code}'
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.content, "lxml")
        try:
            co_info = ' '.join([text.text.strip() for text in dom.select('#summary_info > p')])
            # 코드 추가해서 df로 만들기 + 행/열 전환 한 다음에 return
            return pd.DataFrame([code, co_info]).T
        except:
            print(f'{code} 데이터가져오기 실패')
    #--------------------------------------------------------------------------------------------
    if param=='all':
        df = pd.concat([get_company_explain_from_naver(code) for code in code_list]).reset_index(drop=True)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        #     result = executor.map(get_company_explain_from_naver, code_list)
        #     df = pd.concat([df for df in result], axis=0)
        #     del result
    else:
        new = conn_db.from_("DB_기업정보", 'FS_update_list')[['KEY','종목코드']]
        old = conn_db.from_("DB_기업정보", '기업설명_naver')['KEY']
        code_list_added = pd.DataFrame(list(set(new['KEY']) - set(old)))
        if len(code_list_added)>0:
            code_list_added = code_list_added.merge(new, left_on=0, right_on='KEY')['종목코드']
            df = pd.concat([get_company_explain_from_naver(code) for code in code_list_added], axis=0)
        else:
            print('업데이트할 내역 없음')
        del new, old, code_list_added
    #--------------------------------------------------------------------------------------------
    # 전처리
    df.rename(columns={0: '종목코드', 1: '종목설명'}, inplace=True)
    df['종목설명'] = df['종목설명'].str.replace('동사는', '').str.strip()
    df = helper.make_keycode(df)  # KEY 컬럼 만들기
    df = df.drop(columns=['종목코드', '종목명']).reset_index(drop=True) # KEY컬럼과 종목설명 컬럼만 남기기
    # 업데이트
    old_df = conn_db.from_('DB_기업정보', '기업설명_naver')
    df = pd.concat([df, old_df], axis=0).drop_duplicates(subset=['KEY'])
    conn_db.to_(df, 'DB_기업정보', '기업설명_naver')
    #--------------------------------------------------------------------------------------------
    print('네이버증권에서 기업설명 가져와서 업데이트 완료')
    print('(소요시간: ' + str(helper.now_time() - start_time)+")")
    del df, old_df
#--------------------------------------------------------------------------------------------------------------------
def update_co_from_krx_n_kind(): # KRX와 KIND에서 받은 기업정보 가져오기
    '''
    KRX와 KIND에서 받은 기업정보 가져오기
    '''
    kosdaq_import = conn_db.from_('KRX&KIND_원본업로드','KRX_코스닥')
    kospi_import = conn_db.from_('KRX&KIND_원본업로드','KRX_코스피')
    kind_import = conn_db.from_('KRX&KIND_원본업로드','KIND_상장법인목록')

    for df in [kosdaq_import,kospi_import,kind_import]:
        df = df[df['종목코드'].notna()].copy()

    kospi_import.insert(column='시장', value='KOSPI', loc=1)
    kosdaq_import.insert(column='시장', value='KOSDAQ', loc=1)
    market_df = pd.concat([kospi_import,kosdaq_import], axis=0, ignore_index=True)
    market_df.drop(columns='번호', inplace=True)

    list_co_info = pd.merge(kind_import, market_df, how='inner', on='종목코드')

    list_co_info['업종코드'] = list_co_info['업종코드'].astype(int)
    list_co_info['업종코드'] = list_co_info['업종코드'].apply(lambda x: '{:06d}'.format(x))

    list_co_info.drop(columns=['업종_y','기업명'], inplace=True)
    list_co_info.rename(columns={'업종_x': '업종',
                                '회사명':'종목명'},inplace=True)
    # 글자랑 쉼표사이 공백이 없는 경우가 있음
    list_co_info['주요제품'] = list_co_info['주요제품'].str.replace(',', ', ')
    # 공란2개를 1개로 처리
    list_co_info['주요제품'] = list_co_info['주요제품'].str.replace('  ', ' ')

    list_co_info['KEY'] = list_co_info['시장'] + list_co_info['종목코드']
    conn_db.to_(list_co_info, 'DB_기업정보', 'from_krx')

    print('KRX, KIND 업데이트 완료')
    #--------------------------------------------------------------------------------------------------------------------
def update_co_from_dart():  # Dart에 있는 기업정보 업데이트
    '''
    Dart에 있는 기업정보 업데이트
    '''
    from helper import dart_api_key
    dart.set_api_key(helper.dart_api_key)
    #------------------------------------------------------------------------------------------
    # 전체 회사 코드 list
    raw_corp_code_list = pd.DataFrame(dart.api.filings.get_corp_code())

    #stock_code가 null인것 제거(비상장사 제외하기)
    filt = raw_corp_code_list.stock_code.notnull()
    listcorp_list = raw_corp_code_list[filt].reset_index(drop=True)

    #기업코드별로 기업정보 가져오기
    corp_info_raw = pd.DataFrame()
    for code in listcorp_list.corp_code[:]:
        # helper.random_sleep()
        temp = dart.api.filings.get_corp_info(code)
        temp = pd.DataFrame.from_dict(temp, orient='index').T
        corp_info_raw = corp_info_raw.append(temp)

    # 제외조건 : status != 000 | corp_cls = 'E'
    filt1 = corp_info_raw['corp_cls'] != 'E'  # 법인유형이 '기타'가 아닌 것만 선택
    filt2 = corp_info_raw['status'] == '000'  # status가 000인것만 선택
    corp_info = corp_info_raw.loc[filt1 & filt2, :].copy()

    # 법인구분 : Y(유가), K(코스닥), N(코넥스), E(기타)_ from 다트홈페이지
    # 영문코드로 되어 있는 corp_cls 컬럼을 한글로 변환
    corp_mkt_map = {'Y': 'KOSPI',
                    'K': 'KOSDAQ',
                    'N': '코넥스',
                    'E': '기타'}
    corp_info['corp_cls'] = corp_info['corp_cls'].map(corp_mkt_map).copy()


    #export할 컬럼만 선택
    cols = ['corp_code', 'corp_name', 'stock_code', 'corp_cls',
            'jurir_no', 'bizr_no', 'adres', 'induty_code', 'est_dt', 'acc_mt']

    # 필요한 컬럼만 선택
    corp_info = corp_info.loc[:, cols].copy()

    #영문으로 되어 있는 컬럼명 한글로 수정
    corp_info.rename(columns={'corp_code': '회사코드',
                            'corp_name': '회사명',
                            'stock_code': '종목코드',
                            'corp_cls': '시장',
                            'jurir_no': '법인등록번호',
                            'bizr_no': '사업자등록번호',
                            'adres': '주소',
                            'induty_code': '업종코드',
                            'est_dt': '설립일자',
                            'acc_mt': '결산월'}, inplace=True)
    corp_info['업종코드기준'] = corp_info['업종코드'].apply(lambda x: len(x))
    mapinfo = {5: '세세분류',
               4: '세분류',
               3: '소분류'}
    corp_info['업종코드기준'] = corp_info['업종코드기준'].map(mapinfo)
    corp_info = helper.make_keycode(corp_info)

    # 업로드
    conn_db.to_(corp_info, 'DB_기업정보', 'from_dart')
    print('DART 기업정보 업데이트 완료')

def update_co_from_fdr():  # Finance DataReader에 있는 기업정보
    '''
    FinanceDataReader에 있는 기업정보
    '''
    df_krx = fdr.StockListing('KRX')  # krx종목별 정보 df 불러오기
    df_krx.rename({'Symbol': '종목코드'}, axis=1, inplace=True)
    # 종목 리스트에서 우량주는 제외. 제외할 종목코드 필터링 방법은 'SettleMonth'컬럼이 null인것
    rows = df_krx['SettleMonth'].notna()
    cols = ['종목코드', 'Sector', 'Industry'] # 사용할 컬럼
    co_info = df_krx.loc[rows, cols].reset_index(drop=True) # 실제 사용할 종목코드가 있는 df
    co_info = helper.make_keycode(co_info)
    conn_db.to_(co_info, 'DB_기업정보', 'from_fdr')
    print('FinanceDataReader 기업정보 업데이트 완료')
    del df_krx, co_info, rows
    #--------------------------------------------------------------------------------------------------------------------
def get_trade_volume_from_krx(from_date, to_date):
    '''
    krx에서 종목별 기관,외국인 거래량 가져오기
    '''
    print(f'KRX에서 {from_date}~{to_date} 기관과 외국인 거래량 가져오기 시작')
    folder = r"C:\Users\bong2\Downloads\\"
    save_folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\KRX_80021_기관외국인순매수추이\\"
    code_list = conn_db.from_("DB_기업정보", 'FS_update_list')[['종목명','종목코드']]
    #---------------------------------------------------------------------------------
    def change_dates(from_date, to_date):
        # 시작날짜
        xpath = "//*[starts-with(@id,'fromdate')]"
        driver.find_element_by_xpath(xpath).clear()
        driver.find_element_by_xpath(xpath).send_keys(from_date)
        # 종료날짜
        xpath = "//*[starts-with(@id,'todate')]"
        driver.find_element_by_xpath(xpath).clear()
        driver.find_element_by_xpath(xpath).send_keys(to_date)
    #---------------------------------------------------------------------------------
    driver = webdriver.Chrome()
    # 종목별 기관, 외국인 거래량 가져오기
    codes = code_list['종목코드'].tolist()
    file_count = len(glob(folder+'*.csv'))
    driver.get('http://marketdata.krx.co.kr/mdi#document=13020304')
    for code in codes:
        from_date = '20190101'
        to_date = '20191231'
        change_dates(from_date, to_date)
        code = 'A'+ code
        driver.find_element_by_xpath("//*[starts-with(@id,'isu_')]").clear() # 코드명 비우기
        # 코드 검색
        driver.find_element_by_xpath("//*[starts-with(@id,'finderbtn')]").click() # 검색 클릭
        # 검색창 loading확인
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/dl[1]/dd/div[2]/div/div/dl/dd/div/div[1]/div[1]/div[2]/div/div/table/tbody/tr/td[2]/a'
        element = WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        # 검색코드 입력
        xpath = "//*[starts-with(@id,'searchText')]"
        driver.find_element_by_xpath(xpath).send_keys(code)
        time.sleep(1)
        # 검색결과 선택
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/dl[1]/dd/div[2]/div/div/dl/dd/div/div[1]/div[1]/div[2]/div/div/table/tbody/tr/td[2]/a'
        driver.find_element_by_xpath(xpath).click() #
        # 조회버튼 클릭
        driver.find_element_by_xpath("//*[starts-with(@id,'btnidc')]").click()
        #---------------------------------------------------------------------------------------------------
        # load 확인
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/div[1]/div/div[1]/div[1]/div[1]'
        element = WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        time.sleep(1)
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/div/span/button[3]'
        driver.find_element_by_xpath(xpath).click() # csv 다운로드 클릭
        # 파일명 변경
        while len(glob(folder+'*.csv'))==file_count:
            time.sleep(1)
        time.sleep(2)
        if len(glob(folder+'data*.csv'))>0:
            os.rename(folder+"data.csv", folder+f"{code}_{from_date}_{to_date}.csv")
        time.sleep(1.5)
        file_count = len(glob(folder+'*.csv'))
        #---------------------------------------------------------------------------------------------------
        from_date = '20200101'
        to_date = '20200823'
        change_dates(from_date, to_date)
        driver.find_element_by_xpath("//*[starts-with(@id,'btnidc')]").click() # 조회버튼 클릭
        # load 확인
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/div[1]/div/div[1]/div[1]/div[1]'
        element = WebDriverWait(driver,100).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        time.sleep(1)
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/div/span/button[3]'
        driver.find_element_by_xpath(xpath).click() # csv 다운로드 클릭
        # 파일명 변경
        while len(glob(folder+'*.csv'))==file_count:
            time.sleep(1)
        time.sleep(2)
        if len(glob(folder+'data*.csv'))>0:
            os.rename(folder+"data.csv", folder+f"{code}_{from_date}_{to_date}.csv")
        time.sleep(1.5)
        file_count = len(glob(folder+'*.csv'))
        #---------------------------------------------------------------------------------
    df = pd.DataFrame()
    for file in glob(folder+'*.csv'):
        temp = pd.read_csv(file, encoding='utf-8')
        # 파일명에 있는 코드부분을 컬럼으로 추가
        temp['종목코드'] = file.split('_')[0].split('\\')[-1][1:]
        df = df.append(temp)
    df.rename(columns={'년/월/일':'날짜'}, inplace=True)
    filt = df['종가'].notna()
    df = df.filt[filt].copy()
    df = df.drop_duplicates(subset=['종목코드','날짜']).reset_index(drop=True)
    df = helper.remove_str_from_colname(df,'(주)')
    cols = ['종가', '대비','거래량',
            '기관_매수량', '기관_매도량', '기관_순매수',
            '외국인_매수량', '외국인_매도량','외국인_순매수']
    for col in cols:
        df[col] = df[col].str.replace(',','')
        df[col] = df[col].astype(float)
    # 저장하고 다운로드 폴더에 있는 모든 파일 삭제
    df.to_pickle(save_folder+'기관+외국인거래실적.pkl')
    del df
    helper.del_all_files_in_download()
    #---------------------------------------------------------------------------------
def get_per_per_dividends_from_krx(): # KRX 30009_PERPBR배당수익률에서 가져오기
    dates = conn_db.from_('DI_index','일별날짜')[['날짜']]
    dates = dates[dates['날짜'].notna()].copy()
    dates = dates['날짜'].unique().tolist()[1:]

    file = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\KRX_30009_PERPBR배당수익률\KRX_perpbr배당수익률_취합본.pkl"
    df = pd.read_pickle(file)
    exclude_date = ['20200817','20200505','20200501','20200430','20200415','20200127','20200124','20200101','20191231','20191225',
                    '20191009','20191003','20190913','20190912','20190815','20190606','20190506','20190501','20190301','20190206',
                    '20190205','20190204','20190101','20181231','20181225','20181009','20181003','20180926','20180925','20180924',
                    '20180613','20180522','20180507','20180501','20180216','20180215','20171229','20171225','20171009','20171006',
                    '20171005','20171004','20171003','20171002','20170509','20170503','20170501','20170130','20170127','20161230',
                    '20161003','20160916','20160915','20160914','20160506']
    dates = list(set(dates) - set(df['일자'].unique().tolist()) -set(exclude_date))
    del exclude_date, df
    dates.sort(reverse=True)
    #-------------------------------------------------------------------------------------------------
    folder = r"C:\Users\bong2\Downloads\\"
    file_count = len(glob(folder+'*.csv'))
    driver = webdriver.Chrome()
    time.sleep(3)
    driver.get('http://marketdata.krx.co.kr/mdi#document=13020401')
    time.sleep(10)
    for date in dates:
        # 날짜변경
        driver.find_element_by_xpath("//*[starts-with(@id,'schdate')]").clear()
        driver.find_element_by_xpath("//*[starts-with(@id,'schdate')]").send_keys(date)
        helper.random_sleep()
        time.sleep(2)
        # 조회 클릭
        driver.find_element_by_xpath("//*[starts-with(@id,'btnid')]").click()
        helper.random_sleep()
        time.sleep(20)
        # csv 다운로드 클릭
        xpath = '/html/body/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div/fieldset/form/div/span/button[4]'
        driver.find_element_by_xpath(xpath).click()
        time.sleep(2)
        while len(glob(folder+'*.csv')) == file_count:
            time.sleep(1)
        file_count = len(glob(folder+'*.csv'))
        helper.random_sleep()
    driver.quit()
    del dates
    #-------------------------------------------------------------------------------------------------
    # KRX_perpbr배당수익률 취합후 전처리
    folder = r"C:\Users\bong2\Downloads\\"
    files = glob(folder + '*.csv')
    df = pd.concat([pd.read_csv(file, encoding='utf-8') for file in files],ignore_index=True)
    cols = ['게시물  일련번호','총카운트','관리여부']
    df = df.drop_duplicates().drop(columns=cols)

    cols = ['일자','종목코드','종목명']
    df = df.melt(id_vars=cols, var_name='구분', value_name='값')
    df['일자'] = df['일자'].str.replace('/','')
    df['값'] = df['값'].fillna(0)
    df['값'] = df['값'].apply(lambda x : str(x).replace(',','') if ','in str(x) else str(x))
    df['값'] = df['값'].apply(lambda x : str(x).replace('-','0') if '-'in str(x) else str(x))
    df['값'] = df['값'].astype(float)

    df = df.pivot_table(index=cols, columns='구분', values='값').reset_index()
    df.columns.name=None

    df['배당수익률'] = df['배당수익률'].apply(lambda x : x/100)
    cols = ['종가','주당배당금','BPS','EPS']
    for col in cols:
        df[col] = df[col].astype(int)

    file = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\KRX_30009_PERPBR배당수익률\KRX_perpbr배당수익률_취합본.pkl"
    old_df = pd.read_pickle(file)
    df = pd.concat([df, old_df])
    cols =['일자','종목코드','종목명']
    df = df.drop_duplicates(subset=cols).reset_index(drop=True)
    df.to_pickle(file)
    helper.del_all_files_in_download()
    #----------------------------------------------------------------
    code_list = conn_db.from_('DB_기업정보', 'FS_update_list')[['종목코드']]
    filt = df['종목코드'].isin(code_list['종목코드'])
    df = df.loc[filt].copy().reset_index(drop=True)
    df = helper.make_keycode(df)
    df.drop(columns=['종목코드', '종목명'], inplace=True)
    df.rename(columns={'일자': '날짜'}, inplace=True)
    # 저장
    folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\00_CSV_pickle\\"
    conn_db.export_(df,folder + "KRX_perpbr배당수익률")
    del df, filt, code_list, old_df, dates
    print('KRX_perpbr배당수익률 완료')
#--------------------------------------------------------------------------------------------------------------------
def get_all_co_info(): # 다른 출처에서 가져온 기업정보 전체 취합
    '''
    다른 출처에서 가져온 기업정보 전체 취합
    '''
    # 출처별 기업정보 합치기 전에 컬럼 정리
    # KRX
    co_info = conn_db.from_('DB_기업정보', 'from_krx')
    co_info.drop(columns={'대표자명', '홈페이지','주소','대표전화'}, inplace=True)
    co_info.rename(columns={'업종': '업종_krx'}, inplace=True)
    # 네이버 업종
    naver_industry = conn_db.from_('DB_기업정보', 'from_naver_industry')[['KEY', 'industry']]
    naver_industry.rename(columns={'industry': '업종_naver'}, inplace=True)
    # 네이버 기업설명
    co_explain = conn_db.from_('DB_기업정보', '기업설명_naver')[['KEY', '종목설명']]
    # 네이버 테마
    naver_theme = conn_db.from_('DB_기업정보', 'from_naver_theme2')[['KEY','테마명']].drop_duplicates()
    # DART
    co_dart = conn_db.from_('DB_기업정보', 'from_dart')[['KEY', '회사코드', '회사명', '업종코드기준','업종코드']]
    #------ ------ ------ ------ ------
    # 하나로 합치기
    list_of_dfs = [naver_industry, naver_theme, co_explain, co_dart]
    for dfs in list_of_dfs:
        co_info = co_info.merge(dfs, on='KEY', how='left')
    #3번 파일 정리
    co_info['회사명'] = co_info['회사명'].fillna(co_info['종목명']) # 회사명이 null이면 종목명으로 채우기
    co_info.rename(columns={'업종코드_x': '업종코드_krx',
                            '업종코드_y': '업종코드_dart'}, inplace=True)
    # 오류수정 ---------------------------------------------------------
    co_info.loc[co_info['종목명']=='바른손','결산월'] = '03월'
    # 제외할 리스트에 있는 것들 제외
    # gfile.worksheet_by_title('취합본').clear("*") # 미리 시트 clear 해놓기
    exclude_list = conn_db.from_('DB_기업정보', '제외할list')['종목코드'].drop_duplicates()
    filt = co_info['종목코드'].isin(exclude_list)
    co_info = co_info.loc[~filt].copy()
    # 코넥스 제외
    filt = co_info['시장'] !='코넥스'
    co_info = co_info.loc[filt].copy()
    #--------------------------------------------------------------------------------------------
    # 컬럼순서 정리하고 fnguide에서 가져온 내용 합친 다음 업로드
    df_fnguide = conn_db.from_('DB_기업정보','from_fnguide_기업정보').drop(columns=['종목코드','종목명'])
    df_fnguide.rename(columns={'내용':'실적내용', '요약':'실적요약',
                                'FICS':'FICS 업종','KRX':'업종_krx2'}, inplace=True)
    # 컬럼순서
    cols = [ 'KEY', '종목코드', '종목명', '시장', '업종_krx', '업종_krx2','업종_naver',
            'FICS 업종', '테마명', '주요제품', '종목설명','실적요약', '실적내용', '기준날짜',
            '업종코드_dart', '업종코드기준', '업종코드_krx', '상장일', '결산월', '지역',
            '상장주식수(주)', '자본금(원)','액면가(원)', '통화구분', '회사코드', '회사명']
    co_info = co_info.merge(df_fnguide, on='KEY', how='left')[cols]
    conn_db.to_(co_info, 'DB_기업정보', '취합본')
    print('전체 기업정보 업데이트 완료')
    # 업종별 회사를 산업/업종 master에 업로드. 업종별 광공업지수와 비교분석하기 위함
    conn_db.to_(co_info.groupby(['업종_krx'])['종목명'].apply(', '.join).reset_index(),
                    'Master_산업,업종', '업종별회사')
    del list_of_dfs, naver_industry, naver_theme, cols, co_explain, co_dart, exclude_list, df_fnguide
    #--------------------------------------------------------------------------------------------
    # 한국표준산업별로 종목 구분하기
    use_cols = ['KEY', '종목명', '업종_krx', '업종_krx2', '업종_naver',
                'FICS 업종', '테마명', '업종코드_dart', '업종코드기준']
    co_info = co_info[use_cols]
    df_industry = conn_db.from_('Master_산업,업종','한국표준산업분류')
    #-------
    def clean_df(string):
        temp = co_info.loc[co_info['업종코드기준'] == string].copy()
        return temp.merge(df_industry, left_on='업종코드_dart', right_on=f'{string}_코드', how='left')
    #-------
    df_1 = clean_df('소분류')
    df_2 = clean_df('세분류')
    df_3 = clean_df('세세분류')
    list_of_dfs = [df_1, df_2, df_3]
    for dfs in list_of_dfs:
        dfs = helper.remove_str_from_colname(helper.drop_column(dfs,'_코드'),'_항목명')
    df_1 = df_1.drop(columns=['세분류', '세세분류']).drop_duplicates().reset_index(drop=True)
    df_2 = df_2.drop(columns=['세세분류']).drop_duplicates().reset_index(drop=True)
    # 컬럼순서 정리
    class_cols = ['대분류', '중분류', '소분류', '세분류', '세세분류']
    use_cols = use_cols + class_cols
    df = pd.concat([df_1, df_2, df_3])[use_cols].reset_index(drop=True)
    for col in class_cols:
        df[col] = df[col].str.split('(',expand=True)[0]
    conn_db.to_(df, 'Master_산업,업종','한국표준산업분류별_종목')
    print('한국표준산업분류별로 종목 분류해서 Master_산업 시트에 업로드 완료')
    del df_1, df_2, df_3, list_of_dfs, co_info, df_industry, df, use_cols, class_cols

    #----------------
    # 기업정보 취합본 + 한국표준산업분류별_종목 + 업종별 설명 + 개별테마 + 아이투자_기업정보까지 모두 다 합친 것
    # 1.전체 취합본 불러오기
    cols = ['업종코드_krx','통화구분','액면가(원)', '상장주식수(주)','자본금(원)','회사코드','기준날짜']
    df = conn_db.from_('DB_기업정보','취합본').drop(columns=cols)
    # 2.아이투자_기업정보 불러와서 구분별로 컬럼 만들기
    df_description =  conn_db.from_('DB_기업정보','from_아이투자_기업정보')[['KEY','구분','내용']]
    df_all = pd.DataFrame()
    for x in df_description['구분'].unique():
        temp = df_description[df_description['구분']==x].rename(columns={'내용':x}).drop(columns='구분')
        if len(df_all)==0:
            df_all = df_all.append(temp)
        else:
            df_all = df_all.merge(temp, on='KEY')
    df_all = df_all.rename(columns={'주요제품':'주요제품 (상세)'}).drop_duplicates()
    # 3.한국표준산업분류별_종목 불러오기
    cols = ['KEY','대분류','중분류','소분류','세분류','세세분류']
    df_industry = conn_db.from_('Master_산업,업종','한국표준산업분류별_종목')[cols]
    # 4. KRX업종별 종목 가져오기
    co_by_industry = conn_db.from_('Master_산업,업종', '업종별회사')
    co_by_industry.rename(columns={'종목명':'krx업종내종목'}, inplace=True)
    # 5. KRX업종별 설명 가져오기
    cols = ['세세분류_코드','설명']
    name_map = {'세세분류_코드':'업종코드_dart','설명':'업종설명'}
    industry_info = conn_db.from_("Master_산업,업종", "한국표준산업분류_세부")[cols].rename(columns=name_map)
    # 6.전체 join해서 하나로 만들어서 업로드
    df = df.merge(df_industry, on='KEY', how='inner')
    df = df.merge(df_all, on='KEY', how='left')
    df = df.merge(co_by_industry, on='업종_krx', how='left')
    df = df.merge(industry_info, on='업종코드_dart',  how='left')
    # cols = ['KEY', '종목코드','종목명',  '시장',  '종목설명', '업종_krx', '업종설명', 'krx업종내종목', '업종코드기준',
    #         '대분류', '중분류', '소분류', '세분류', '세세분류', '업종_krx2', '업종_naver', 'FICS 업종',
    #         '테마명', '주요제품', '주요제품 (상세)', '실적요약', '실적내용', '사업환경',
    #         '경기변동',  '원재료', '실적변수', '재무리스크', '신규사업', '상장일', '결산월', '지역', '회사명' ]
    # df = df.sort_values(by=['KEY']).reset_index(drop=True)[cols]
    conn_db.to_(df, 'DB_기업정보','총괄')
    print('기업정보 전체 업데이트 완료')
    del df, df_all, df_description, df_industry, industry_info
#--------------------------------------------------------------------------------------------------------------------
def get_stock_data(param):
    code, start_date, end_date = param
    df = fdr.DataReader(code, start_date, end_date)
    if len(df) > 0:
        df.reset_index(inplace=True)
        df['종목코드'] = str(code)
        return df
    else:
        pass
    #------------------------------------------------------------------------------------------------------------------
def update_stock_price(start_date, end_date, update_name):  # 주가 데이터 업데이트후 hyper, pickle 저장
    '''
    주가 데이터 업데이트후 hyper, csv 저장
    '''
    start_time = helper.now_time()
    code_list = conn_db.from_('DB_기업정보','FS_update_list')[['종목코드']]
    stock_data_path = conn_db.get_path('stock_raw')

    code_list.loc[:, 'start_date'] = start_date
    code_list.loc[:, 'end_date'] = end_date
    params_list = code_list.values.tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(get_stock_data, params_list)
    df = pd.concat([result for result in results], axis=0)
    df = df.drop_duplicates().reset_index(drop=True)
    df = helper.make_keycode(df)

    df.to_pickle(stock_data_path+ f"{update_name}_주가.pkl")
    print(f'{update_name} 주가 업데이트 완료. 소요시간: ' + str(helper.now_time() - start_time))

    #--------------- --------------- --------------- ---------------
    # 받은 데이터 정리
    files = glob(stock_data_path + '*_주가.pkl')
    df = pd.concat([pd.read_pickle(file)for file in files]).reset_index(drop=True)

    # 과거에 받은거에는 현재시점에서 필요없는 종목코드가 포함되어 있을수 있기 때문에 필터링
    filt = df['종목코드'].isin(code_list['종목코드'])
    df = df.loc[filt].copy()
    conn_db.export_(df, '주가취합본')
    print('전체 추가 데이터 취합완료')
#------------------------------------------------------------------------------------------------------------------
