import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
from glob import glob
import helper
import conn_db
# import pantab, platform

#----------------------------------------------------------------
dt = helper.now_time()
suffix = helper.get_time_suffix()
user_agent = helper.user_agent
max_workers = 3
code_list = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
# fsratio_class = conn_db.from_('from_fnguide', '재무비율_항목정리').drop_duplicates()
# invest_class = conn_db.from_('from_fnguide', '투자지표_항목정리').drop_duplicates()
#----------------------------------------------------------------
# fnguide 기업정보 가져올때 결과물 넣을 dataframes
company_info = pd.DataFrame() # 업종, business summary
financial_highlights = pd.DataFrame() # 재무제표
sales_mix = pd.DataFrame() # 제품별 매출비중 가장 최근
market_share = pd.DataFrame() # 시장점유율 가장 최근
cogs_n_oc = pd.DataFrame() # 판관비율추이, 매출원가율추이
export_n_domestic = pd.DataFrame() # 수출 및 내수 구성
#----------------------------------------------------------------
# 아이투자 기업정보 기업정보 가져올때 결과물 넣을 dataframes
company_description = pd.DataFrame()  # 전체 내용있는것
raw_material_1_df = pd.DataFrame() # 원재료_가로형
raw_material_2_df = pd.DataFrame() # 원재료_세로형
product_1_df = pd.DataFrame() # 제품_가로형
product_2_df = pd.DataFrame()  # 제품_세로형
#----------------------------------------------------------------
# 아이투자 투자지표 가져올때 결과물 넣을 dataframes
df_short = pd.DataFrame() # 가장 최근값 테이블
df_5yr = pd.DataFrame() # 5년 평균 테이블
df_tables = pd.DataFrame() # 페이지 아래에 있는 전체 표(연환산,연간,분기)
#----------------------------------------------------------------
folder_fn = conn_db.get_path('folder_fn')
folder_fn_backup = conn_db.get_path('folder_fn_backup')
folder_naver = conn_db.get_path('folder_naver')
folder_naver_backup = conn_db.get_path('folder_naver_backup')
folder_itooza = conn_db.get_path('folder_itooza')
folder_itooza_backup = conn_db.get_path('folder_itooza_backup')

def clean_numeric_value(df): # ['값'] 컬럼을 문자열에서 숫자로 수정
    try:
        filt1 = df['날짜'].notna() # 날짜 컬럼 확인. 날짜컬럼 없으면 except문으로 이동
        filt2 = df['값']!= '-' # 금액에 null 대신 '-'로 되어 있는거 제거
        filt3 = df['값'].apply(len) > 0  # 길이가 0이 안되면 삭제
        filt = filt1 & filt2 & filt3
    except:
        filt1 = df['값']!= '-' # 금액에 null 대신 '-'로 되어 있는거 제거
        filt2 = df['값'].apply(len) > 0  # 길이가 0이 안되면 삭제
        filt = filt1 & filt2
    df = df.loc[filt, :].copy()
    df.reset_index(drop=True, inplace=True)
    # df['값'] = df['값'].str.replace(',', '')
    try:
        df['값'] = df['값'].str.replace(',', '')
        df['값'] = df['값'].str.replace('%', '')
    except:
        pass
    df['값'] = pd.to_numeric(df['값'], errors='coerce')
    df.dropna(subset=['값'], inplace=True)
    return df.reset_index(drop=True)

def drop_duplicate_rows(df, old_df, check_cols):
    df = pd.concat([df, old_df], axis=0)
    # check_cols 기준으로 중복제거 후 return
    return df.drop_duplicates(check_cols).reset_index(drop=True)

def merge_df_all_numbers(): # 아이투자, naver, fnguide 합쳐진 하나의 df만들기
    df_itooza = pd.read_pickle(folder_itooza + '장기투자지표_취합본.pkl')
    df_naver = pd.read_pickle(folder_naver + 'fs_from_naver_최근값만.pkl')
    df_fnguide_fsratio = pd.read_pickle(folder_fn + '2_fsratio_from_fnguide_최근값만.pkl')

    #----------------------------------------------------------------
    # 아이투자 듀퐁ROE 추가
    files = glob(folder_itooza_backup + '*_최근지표*.pkl')
    files.reverse()
    temp = pd.concat([pd.read_pickle(file) for file in files])
    temp = temp.drop_duplicates().reset_index(drop=True)
    temp = helper.make_keycode(temp).drop(columns=['종목코드','종목명'])
    temp['항목'] = temp['항목']+'_r'
    temp = temp.pivot_table(index='KEY',columns='항목', values='값').reset_index()
    temp.columns.name=None
    temp['ROE_r'] = temp['ROE_r']/100

    #--------------------------------------------------------------------
    df = df_itooza.merge(df_naver, on='KEY', how='inner')
    df = df.merge(df_fnguide_fsratio, on='KEY', how='inner')
    df = df.merge(temp, on='KEY', how='inner')
    df = df.merge(conn_db.from_('DB_기업정보','총괄') , on='KEY', how='inner')

    #--------------------------------------------------------------------
    # 네이버업종PER 추가
    industry_per = conn_db.from_('DB_기업정보','네이버업종PER')
    industry_per['업종PER'] = industry_per['업종PER'].astype('float')
    df = df.merge(industry_per, on='업종_naver', how='left')

    # 합친것 저장
    df.to_pickle(conn_db.get_path('장기투자지표_취합본+기업정보총괄')+'.pkl')
    conn_db.to_(df, 'Gfinance_시장data', 'import_장기투자지표_취합본+기업정보총괄')
    
#--------------------------------------------------------------------------------------------------------------------
#FN GUIDE 재무제표
def get_fs_from_fnguide(dom, tp, fstype):  # fnguide 재무제표 가져오기
    fstypes = ['divSonikY','divSonikQ','divDaechaY','divDaechaQ','divCashY','divCashQ']
    fstypes_name = {'divSonikY': '연간손익계산서',
                    'divSonikQ': '분기손익계산서',
                    'divDaechaY': '연간재무상태표',
                    'divDaechaQ': '분기재무상태표',
                    'divCashY': '연간현금흐름표',
                    'divCashQ': '분기현금흐름표'}
    report_name = {'B':'별도',
                    'D':'연결'}
    time.sleep(1)
    try:
        datas = dom.select(f'#{fstype} > table > tbody > tr') # 계정별로 data들어 있는 것
        header_data = dom.select(f'#{fstype} > table > thead > tr > th')  # 컬럼 header
        header_data = [data.text for data in header_data]
        if '전년동기' in header_data:
            loc = int(header_data.index('전년동기')) - 1  # 컬럼에서 전년동기 컬럼의 이전컬럼 위치
            yyyy = str(int(header_data[loc][:4]) - 1)  # 년도부분 가져오기
            mm = header_data[loc][-2:]  # 월부분 가져오기
            yyyymm = yyyy + '/' + mm  # 연월 날짜 만들어주기
            header_data[loc+1] = yyyymm  # '전년동기'를 날짜값으로 변경
        else:
            pass
        df = pd.DataFrame(header_data)  # 컬럼만 들어가 있는 df
        for i in range(len(datas)-1):
            if i == 0:
                # 대분류 계정명 , data[0] 일때만 해당됨
                account_nm = [datas[0].select('div')[0].text]
                # 대분류 계정의 컬럼별 값
                data = [data.text for data in datas[0].select('td')]
            else:
                # 소분류 계정명 , data[0] 이상 일때만 해당됨
                account_nm = [datas[i].select('th')[0].text.strip()]
                # 소분류 계정의 컬럼별 값
                data = [data.text for data in datas[i].select('td')]
            row = pd.DataFrame(account_nm + data)  # 계정별로 행 전체
            df = pd.concat([df, row], axis=1)
        df = df.T
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        first_col = df.columns.tolist()[0]
        df['재무제표기준'] = first_col  # 재무제표기준 컬럼으로 추가
        df.rename(columns={first_col: '항목'}, inplace=True)  # 첫 컬럼명을 '항목'으로 수정
        df = df.reset_index(drop=True).reset_index().rename(columns={'index': '항목순서'}) # 항목순서 컬럼 만들기 위해 2번 reset_index
        if '전년동기(%)' in df.columns.tolist():
            df.drop(columns='전년동기(%)', inplace=True)
        else:
            pass
        df = df.melt(id_vars=['재무제표기준', '항목', '항목순서'], var_name='날짜', value_name='값')
        df['재무제표기준'] = fstypes_name[fstype]
        df['연결/별도'] = report_name[tp]
        return df
    except:
        pass
    #--------------------------------------------------------------------------------------------------------------------
def get_all_fs_from_fnguide(code):  # fnguide 재무제표 가져오기
    '''
    fnguide에서 재무제표 가져오기
    '''
    fstypes = ['divSonikY', 'divSonikQ', 'divDaechaY', 'divDaechaQ', 'divCashY', 'divCashQ']
    report_types = ['B', 'D']
    report_name = {'B': '별도', 'D': '연결'}
    df_all = pd.DataFrame()  # 연결/분기 재무제표 넣을 df
    # 연결/별도 기준별로 data 가져오기
    for report_type in report_types:
        url = f"http://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB={report_type}&NewMenuID=103&stkGb=701"
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.text, "html.parser")
        try:
            df = pd.concat([get_fs_from_fnguide(dom, report_type, fstype) for fstype in fstypes])
            df_all = df_all.append(df)
        except:
            pass
    try:
        df_all['항목'] = df_all['항목'].str.replace('계산에 참여한 계정 펼치기', '')
        df_all['주기'] = df_all['재무제표기준'].str[:2]
        df_all['종목코드'] = code
        df_all['재무제표종류'] = df_all['재무제표기준'].str[2:]
        df_all.drop(columns='재무제표기준', inplace=True)
        # print(f'{code} 완료')
        return df_all.drop_duplicates().reset_index(drop=True)
    except:
        print(f'{code} 데이터가져오기 실패')
    #--------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_fnguide_fs(param='all'):  # fnguide 재무제표 업데이트
    global code_list
    global max_workers
    file = conn_db.get_path('fs_from_fnguide_raw')+".pkl"
    if param=='all':
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            result = executor.map(get_all_fs_from_fnguide, code_list)
            df = pd.concat([df for df in result], axis=0)
    else:
        df = pd.read_pickle(file)
        new_code = list(set(df['종목코드'].unique()) - set(conn_db.from_('DB_기업정보','취합본')['종목코드']))
        df = pd.concat([get_all_fs_from_fnguide(code) for code in new_code], axis=0)
        del new_code
    df = clean_numeric_value(df)
    df = helper.make_keycode(df)  # KEY 컬럼 추가

    # 취합본 업데이트 후 저장
    old_df = pd.read_pickle(file)
    cols = ['항목','항목순서','날짜','연결/별도','주기','종목코드','종목명','KEY','재무제표종류']
    df = drop_duplicate_rows(df, old_df, cols)
    df = df[df['종목코드'].isin(code_list)].copy()
    # 새로 합쳐진것 저장
    df.to_pickle(file)
    #--------------------------------------------------------------------------------------------------------------------
#FN GUIDE 재무비율
def get_fsratio_from_fnguide(code):  # fnguide 재무비율 가져오기
    '''
    fnguide 재무비율 가져오기
    '''
    time.sleep(1)
    report_types = ['B', 'D'] # {'B': '별도', 'D': '연결'}
    df_all = pd.DataFrame()  # 전체 재무제표 넣을 df
    # 연결/별도 기준별로 data 가져오기
    for report_type in report_types:
        url = f"http://comp.fnguide.com/SVO2/asp/SVD_FinanceRatio.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB={report_type}&NewMenuID=104&stkGb=701"
        r = requests.get(url, headers={'User-Agent': user_agent})
        dom = BeautifulSoup(r.text, "html.parser")
        # 컬럼정보와 데이터가 들어가 있는 변수 생성
        header_info = dom.select('#compBody > div.section.ul_de > div > div.um_table > table > thead')
        all_data = dom.select('#compBody > div.section.ul_de > div > div.um_table > table')
        # 데이터 변수 안에 있는 거에서 df로 정리하기
        temp = pd.DataFrame()
        for report in range(2): # 0은 연간, 1은 분기 데이터를 가져옴
            try:
                datas = all_data[report]  # 데이터가 들어 있는 곳
                date_cols = header_info[report]  # 컬럼 header가 들어 있는 곳
                date_cols = [data.text.strip() for data in date_cols.select('th')]  # 날짜 컬럼 header
                account_nm = [x.text.strip() for x in datas.select('tr > th > div')]  # 계정명칭
                df = pd.DataFrame(account_nm)
                data_values = [x.text.strip() for x in datas.select('td')] # 값만 들어가 있는 컬럼
                lap = len(date_cols)-1
                for i in range(lap):
                    temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
                    df = pd.concat([df, temp_df], axis=1)
                df.columns = date_cols
                first_col = df.columns.tolist()[0]
                df.rename(columns={first_col: '항목'}, inplace=True)  # 첫 컬럼명을 '항목'으로 수정
                df['항목'] = df['항목'].str.replace('계산에 참여한 계정 펼치기', '')
                # 항목 명칭 정리된거랑 merge
                # acc_class = fsratio_class
                # df = df.merge(acc_class, on='항목',how='left').drop_duplicates()
                df = df.melt(id_vars='항목', var_name='날짜', value_name='값')
                df = df.dropna().drop_duplicates().reset_index(drop=True)
                df['재무제표기준'] = first_col  # 재무제표기준 컬럼으로 추가
                df['주기'] = '연간' if report == 0 else '분기' # 연간/분기 컬럼 추가
                temp = temp.append(df) # 연간 / 분기 df 추가
            except:
                print(f'{code} {report} {report_type} 데이터가져오기 실패')
                pass
        if len(temp)>1:
            df_all = df_all.append(temp, ignore_index=True) # 연결 / 별도 df 추가
        else:
            # print(f'{code} {report_type} 데이터가져오기 실패')
            pass
    try:
        df_all = df_all
        df_all['종목코드'] = code
        df_all = df_all[['종목코드', '재무제표기준', '주기', '항목', '날짜', '값']]
        return df_all.drop_duplicates().reset_index(drop=True)
    except:
        pass
    #--------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_fnguide_fsratio(param='all'):  # fnguide 재무비율 업데이트
    global code_list
    global max_workers
    file = conn_db.get_path('fsratio_from_fnguide_raw')+'.pkl'
    if param=='all':
        df = pd.concat([get_fsratio_from_fnguide(code) for code in code_list], axis=0)
        # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        #     result = executor.map(get_fsratio_from_fnguide, code_list)
        # df = pd.concat([df for df in result], axis=0)
        # del df
    else:
        df = pd.read_pickle(file)
        new_code = conn_db.from_('DB_기업정보', '취합본')['종목코드']
        new_code = list(set(df['종목코드'].unique()) - set(new_code))
        try:
            df = pd.concat([get_fsratio_from_fnguide(code) for code in new_code], axis=0)
            del new_code
        except:
            print('업데이트할 내역 없음')
    if len(df)>0:
        df = clean_numeric_value(df) # 값 컬럼 정리
        # 취합본 불러와서 합치기
        old_df = pd.read_pickle(file)
        df['항목'] = df['항목'].apply(lambda x : x.split(u'\xa0')[-1].strip() if '\xa0' in x else x.strip())
        cols = ['항목','날짜','재무제표기준','주기','종목코드']
        df = drop_duplicate_rows(df, old_df, cols)
        df.to_pickle(file)
        del df, old_df, file
    else:
        print('업데이트할 내역 없음')
    #----------- ----------- ----------- ----------- ----------- ----------- ----------- -----------
def clean_fsratio_from_fnguide(): # fnguide 재무비율 전처리
    df = pd.read_pickle(conn_db.get_path('fsratio_from_fnguide_raw')+'.pkl')
    # KEY컬럼 만들기
    df = helper.make_keycode(df)
    #------------------------------------------------------------------------------------

    maper = {'IFRS(연결)':'연결', 'GAAP(연결)':'연결',
            'IFRS(개별)':'개별', 'GAAP(개별)':'개별', 'IFRS(별도)':'개별'}
    df['연결/별도'] = df['재무제표기준'].map(maper)
    df.drop(columns='재무제표기준', inplace=True)
    df['temp_key'] = df['KEY']+df['연결/별도'] # 일시 key컬럼
    #------------------------------------------------------------------------------------

    #주재무제표만 남기기 위해서 naver에서 가져온 종목별 주재무제표 df와 inner join
    temp_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
    maper = {'IFRS연결':'연결', 'GAAP연결':'연결',
            'IFRS개별':'개별', 'GAAP개별':'개별', 'IFRS별도':'개별'}
    temp_fs['연결/별도'] = temp_fs['재무제표기준'].map(maper)
    temp_fs['temp_key'] = temp_fs['KEY']+temp_fs['연결/별도']
    temp_fs.drop(columns=['재무제표기준','KEY','연결/별도'], inplace=True)
    df = df.merge(temp_fs, on='temp_key', how='inner').drop(columns=['temp_key'])
    del temp_fs
    #------------------------------------------------------------------------------------

    # DB_기업정보 FS_update_list에 있는 코드만 필터링
    filt = (df['값']!=0.0) & (df['종목코드'].isin(code_list))
    df = df[filt].copy()
    df = df.sort_values(by='날짜', ascending=False)
    df = df.dropna(axis=1, how='all') # 전체가 null인 경우는 삭제
    #--------------------------------------------------------------------------

    # 항목에 '(-1Y)'가 있으면 전년도 값임. 날짜부분을 실제 -1Y에 해당하는 날짜로 변경
    filt = df['항목'].str.contains('-1Y')
    df_all = df.loc[~filt, :].copy() # 전년도 값이 아닌, 본래 년도의 값만 들어 있는 df
    df_all.drop_duplicates(inplace=True)

    df_temp = df.loc[filt, :].copy() # 전년도 값만 들어 있는 df
    year_part = df_temp['날짜'].str[:4].tolist()
    year_part = [str(int(x)-1) for x in year_part] # '전년도' 구하기
    quarter_part = df_temp['날짜'].str[-3:].tolist() # '월부분' 발라내기
    df_temp['날짜'] = [x+y for x, y in zip(year_part, quarter_part)] # 새로 날짜 만들어 주기
    df_temp['항목'] = df_temp['항목'].str.replace('\(-1Y\)', '').str.strip()

    df = pd.concat([df_all, df_temp]).drop_duplicates()
    del df_temp, df_all
    #--------------------------------------------------------------------------

    df.drop(columns=['종목코드','종목명'], inplace=True)
    # 항목을 컬럼으로 옮기기
    dcols = ['KEY', '연결/별도', '주기', '날짜']
    df = df.pivot_table(index=dcols, columns='항목', values='값').reset_index()
    df.columns.name = None
    # 비율인 컬럼은 100으로 나누어 주어야 함. *100이 된 상태로 들어가 있음
    matcher = ['RO', '률', '율']
    all_cols = df.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df[col] = df[col]/100
    # 연율화는 나누기 100을 하면 안되기 때문에 다시 곱하기 100
    matcher = ['연율화']
    all_cols = df.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df[col] = df[col]*100
    #--------------------------------------------------------------------------

    df.to_pickle(folder_fn + "2_fsratio_from_fnguide_시계열.pkl")
    print('fnguide 재무비율 시계열용 pickle 저장완료')
    #-------------------------------- -------------------------------- --------------------------------

    # 분기/연간에서 가장 최근값만 있는 df만들기
    df = df.melt(id_vars=dcols, var_name='항목', value_name='값').dropna()
    df = df.sort_values(by=dcols, ascending=False).reset_index(drop=True)
    # df = df.groupby(['KEY','주기','연결/별도','항목'], as_index=False).head(1)
    # --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
    def filter_date(df):  # 회사별로 가장 최근 날짜에 해당하는 것만 남기기
        df_all = pd.DataFrame()
        for key in df['KEY'].unique().tolist():
            temp_df = df.loc[df['KEY'] == key]
            if temp_df['주기'].unique() == '분기':
                # 분기인 경우 0번째는 최근분기, 4번째는 전년분기
                date = temp_df['날짜'].unique().tolist()[0]
                filt = temp_df['날짜'] == date
                df_all = df_all.append(temp_df.loc[filt])
                try: # 신규상장된 경우 예전 값이 없을 수도 있음
                    date = temp_df['날짜'].unique().tolist()[4]
                    filt = temp_df['날짜'] == date
                    temp = temp_df.loc[filt].copy()
                    temp['주기'] = '전년분기'
                    df_all = df_all.append(temp)
                except:
                    pass
            #-----------------------------------------------------
            else:  # 연간인 경우 0번째는 분기누적, 1번째는 연간
                date = temp_df['날짜'].unique().tolist()[0]
                filt = temp_df['날짜'] == date
                temp = temp_df.loc[filt].copy()
                temp['주기'] = '분기누적'
                df_all = df_all.append(temp)
                try: # 신규상장된 경우 예전 값이 없을 수도 있음
                    date = temp_df['날짜'].unique().tolist()[1]
                    filt = temp_df['날짜'] == date
                    df_all = df_all.append(temp_df.loc[filt])
                except:
                    pass
        return df_all.reset_index(drop=True)
    # --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
    filt = df['주기']=='분기'
    df_q = df[filt]
    df_y = df[~filt]
    # 연간 + 분기 합치기
    df = pd.concat([filter_date(df_q), filter_date(df_y)])
    del df_q, df_y
    df['항목'] = df['항목'] + " _" +  df['주기']
    df = df.drop(columns=['날짜','주기']).reset_index(drop=True)
    # 행/열 전환하고 저장하기
    df = df.pivot_table(index=['KEY', '연결/별도'], columns='항목', values='값').reset_index()
    df.columns.name = None
    #--------- --------- --------- --------- --------- --------- --------- --------- --------- --------- ---------
    # df.to_pickle(folder_fn + '2_fsratio_from_fnguide_최근값만.pkl')
    conn_db.to_(df, 'from_fnguide', 'fnguide_fsratio_최근값만')
    del df
    print('fnguide 재무비율 최근값만 업로드 완료')
    merge_df_all_numbers() # 전체 취합본 업데이트
    #--------------------------------------------------------------------------------------------------------------------
#FN GUIDE 투자지표
def get_invest_ratio_from_fnguide(code): # fnguide 투자지표 가져오기
    '''
    fnguide 투자지표 가져오기
    '''
    time.sleep(1)
    report_types = ['B', 'D']
    df_all = pd.DataFrame()  # 전체 재무제표 넣을 df
    for report_type in report_types:
        url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=2&gicode=A{code}&cID=&MenuYn=Y&ReportGB={report_type}&NewMenuID=105&stkGb=701'
        r = requests.get(url, headers={'User-Agent': user_agent})
        try:
            dom = BeautifulSoup(r.text, "html.parser")
            all_datas = dom.select('#compBody > div.section.ul_de > div.ul_col2wrap.pd_t25 > div.um_table > table')[0]
            date_cols = [x.text.strip() for x in all_datas.select('tr')[0].select('th')]
            data_values = [x.text.strip() for x in all_datas.select('tr > td')]
            account_cols = []
            lap = len(all_datas.select('div'))
            for i in range(lap):
                try:
                    acc = all_datas.select('div')[i].select('dt')[0].text.strip()
                    account_cols.append(acc)
                except:
                    try:
                        account_cols.append(all_datas.select('div')[i].text.strip())
                    except:
                        pass
            df = pd.DataFrame(account_cols)
            lap = len(date_cols)-1
            for i in range(lap): # 컬럼의 갯수가 5개이기 때문에 매5번째마다 합쳐줌
                temp_df = pd.DataFrame(data_values[i::lap])
                df = pd.concat([df, temp_df], axis=1)
            df.columns = date_cols
            first_col = df.columns.tolist()[0]
            df.rename(columns={first_col: '항목'}, inplace=True)  # 첫 컬럼명을 '항목'으로 수정
            # acc_class = invest_class
            # df = df.merge(acc_class, on='항목', how='left').drop_duplicates()
            df = df.melt(id_vars='항목', var_name='날짜', value_name='값')
            df = df.dropna().drop_duplicates().reset_index(drop=True)
            df['재무제표기준'] = first_col  # 재무제표기준 컬럼으로 추가
            df_all = df_all.append(df)
        except:
            # print(f'{code} {report_type} 데이터가져오기 실패')
            pass
    try:
        df_all['종목코드'] = code
        return df_all.drop_duplicates().reset_index(drop=True)
    except:
        print(f'{code} 데이터가져오기 실패')
    #--------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_fnguide_invest_ratio(param='all'):  # fnguide 투자지표 업데이트
    global code_list
    global max_workers
    start_time = helper.now_time()
    print('fnguide 투자비율 가져오기 시작 ' + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    file = folder_fn_backup + "invest_ratio_from_fnguide_받은원본취합본.pkl"
    # file = conn_db.from_('from_fnguide','fnguide_invest_ratio_원본취합본')
    if param=='all':
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            result = executor.map(get_invest_ratio_from_fnguide, code_list)
            df = pd.concat([df for df in result], axis=0)
    else:
        df = pd.read_pickle(file)
        new_code = list(set(df['종목코드'].unique()) - set(conn_db.from_('DB_기업정보','취합본')['종목코드']))
        df = pd.concat([get_fsratio_from_fnguide(code) for code in new_code], axis=0)
    # 전처리 시작
    if len(df)>0:
        df = clean_numeric_value(df)
        # ------- ------- ------- ------- ------- ------- ------- -------
        # 백업 불러와서 취합본 업데이트
        old_df = pd.read_pickle(file)
        cols = ['항목', '날짜', '재무제표기준', '종목코드']
        df = drop_duplicate_rows(df, old_df,cols)
        df.to_pickle(file)
        # ------- ------- ------- ------- ------- ------- ------- -------
        df['항목'] = df['항목'].str.replace('\(', ' (').str.strip()
        df = df.pivot_table(index=['날짜','종목코드','재무제표기준'], columns='항목', values='값').reset_index()
        df.columns.name = None
        df['배당성향 (현금,%)'] = df['배당성향 (현금,%)']/100
        df.rename(columns={'배당금 (현금) (억원)':'배당금 (억원)'}, inplace=True)
        # DB_기업정보 FS_update_list에 있는 코드만 필터링, KEY컬럼 만들기
        df = helper.make_keycode(df)
        df = df[df['종목코드'].isin(code_list)].copy()
        df = df.dropna(axis=1, how='all') # 전체가 null인 경우는 삭제
        # ------- ------- ------- ------- ------- ------- ------- -------
        maper = {'IFRS 연결':'연결', 'GAAP 연결':'연결',
                'IFRS 개별':'개별', 'GAAP 개별':'개별', 'IFRS 별도':'개별'}
        df['연결/별도'] = df['재무제표기준'].map(maper)
        df.drop(columns='재무제표기준', inplace=True)
        df['temp_key'] = df['KEY']+df['연결/별도'] # 일시 key컬럼

        #주재무제표만 남기기 위해서 naver에서 가져온 종목별 주재무제표 df와 inner join
        temp_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
        maper = {'IFRS연결':'연결', 'GAAP연결':'연결',
                'IFRS개별':'개별', 'GAAP개별':'개별', 'IFRS별도':'개별'}
        temp_fs['연결/별도'] = temp_fs['재무제표기준'].map(maper)
        temp_fs['temp_key'] = temp_fs['KEY']+temp_fs['연결/별도']
        temp_fs.drop(columns=['재무제표기준','KEY','연결/별도'], inplace=True)
        df = df.merge(temp_fs, on='temp_key', how='inner').drop(columns=['temp_key'])
        del temp_fs
        #------------------------------------------------------------------------------------
        df.drop(columns=['종목코드','종목명'],inplace=True)
        df = df.merge(conn_db.from_("DB_기업정보", '총괄'), on='KEY', how='left')
        # 저장
        df.to_pickle(folder_fn + "1_invest_ratio_from_fnguide_시계열.pkl")
        print('fnguide 투자지표 저장완료')
        print('(소요시간: ' + str(helper.now_time() - start_time)+")")
        del df, old_df, cols
    else:
        print('업데이트할 내역 없음')
    #--------------------------------------------------------------------------------------------------------------------
#FN GUIDE 기업정보
def get_fnguide_company_info(code):
    # fnguide 기업정보 결과물 넣을 dataframes---------------
    global company_info # 업종, business summary
    global financial_highlights # 재무제표
    global sales_mix # 제품별 매출비중 가장 최근
    global market_share # 시장점유율 가장 최근
    global cogs_n_oc # 판관비율추이, 매출원가율추이
    global export_n_domestic # 수출 및 내수 구성

    #업종, business summary, 재무제표 공통
    url = f"http://comp.fnguide.com/SVO2/asp/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
    time.sleep(1)
    r = requests.get(url, headers={'User-Agent': user_agent})
    dom = BeautifulSoup(r.text, "html.parser")
    #업종, business summary
    # 업종 가져오기
    temp = []
    for i in [1,2]: # 1=KSE 업종, 2=FICS 업종
        text = dom.select(f'#compBody > div.section.ul_corpinfo > div.corp_group1 > p > span.stxt.stxt{i}')[0]
        temp.append(text.text.replace('\xa0',' ').split(' ',1))
    industry = pd.DataFrame(temp).T
    # 첫행을 컬럼으로 변경하고 원래있던 첫행은 삭제
    industry.columns = industry.iloc[0]
    industry = industry.iloc[1:,].copy().reset_index(drop=True)
    #-------------------------------------

    # Business Summary 가져오기
    # update날짜 앞뒤로 []가 있어서 삭제
    try: # 일부 종목은 Business Summary 없음
        biz_summary_date = dom.select('#bizSummaryDate')[0].text.strip()[1:-1]
        # 제목
        biz_summary_title = dom.select('#bizSummaryHeader')[0].text.replace('\xa0',' ').strip()
        # 내용
        contents =[]
        for i in range(len(dom.select('#bizSummaryContent > li'))):
            contents.append(dom.select('#bizSummaryContent > li')[i].text.replace('\xa0',' ').strip())
        contents = [contents[0] + " " + contents[1]]
        # 합쳐서 하나로 만들기 + 업종도
        contents = pd.DataFrame([biz_summary_date, biz_summary_title, contents]).T
        contents[2] = contents[2][0][0] # 내용 컬럼의 값이 앞뒤로 list[]화 되어 있어서 문자열로 변경
        df = pd.concat([contents, industry], axis=1)
        df['종목코드'] = code
        # 결과물 추가
        company_info = company_info.append(df)
        del contents, biz_summary_date, biz_summary_title, industry, temp
    except:
        pass

    # 재무제표
    # highlight_D_A # 연결 전체, highlight_B_A # 별도 전체 - 사용x
    df_types = {'highlight_D_Y':'연결 연간',
                'highlight_D_Q':'연결 분기',
                'highlight_B_Y':'별도 연간',
                'highlight_B_Q':'별도 분기'}
    df_all = pd.DataFrame()
    for fs_type in df_types.keys():
        try:
            # 날짜 컬럼
            temp = dom.select(f'#{fs_type} > table > thead > tr.td_gapcolor2 > th > div')
            date_cols = []
            for x in [x.text.strip() for x in temp]:
                if len(x)>7:
                    date_cols.append(x.split('추정치')[-1].strip())
                else:
                    date_cols.append(x)
            # 계정명 컬럼
            temp = dom.select(f'#{fs_type} > table > tbody > tr > th')
            account_cols = []
            for acc in [x.text.strip() for x in temp]:
                if acc.count("(",)>1:
                    account_cols.append(acc.split(')',1)[0] + ')')
                else :
                    account_cols.append(acc)
            df = pd.DataFrame(account_cols)
            # data 값
            data_values = [x.text.strip() for x in dom.select(f'#{fs_type} > table > tbody > tr > td')]
            lap = len(date_cols)
            for i in range(lap):
                temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
                df = pd.concat([df, temp_df], axis=1)
            # 항목 컬럼명 추가해서 df 컬럼 만들기
            df.columns = ['항목'] + date_cols
            df['연결/별도'] = df_types[fs_type].split(' ')[0]
            df['연간/분기'] = df_types[fs_type].split(' ')[1]
            df = df.melt(id_vars=['항목', '연결/별도', '연간/분기'], var_name='날짜',value_name='값')
            df = clean_numeric_value(df)
            df_all = df_all.append(df)
        except:
            pass
    try:
        df_all['종목코드'] = code
        # 결과물 추가
        financial_highlights = financial_highlights.append(df_all)
        del df_all, df, temp, data_values
    except:
        pass

    #----- 제품별 매출비중, 시장점유율, 판관비율, 매출원가율, 수출 및 내수구성 공통 ------ ------ ------
    url = f"http://comp.fnguide.com/SVO2/asp/SVD_Corp.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=102&stkGb=701"
    r = requests.get(url, headers={'User-Agent': user_agent})
    dom = BeautifulSoup(r.text, "html.parser")

    #----- 제품별 매출비중 가장 최근 ----- ------ ------ ------ ------ ------ ------ ------ -----
    # 날짜 컬럼
    date_cols = dom.select('#divProduct > div.ul_col2_l > div > div.um_table.pd_t1 > table > thead > tr > th')
    date_cols = [x.text.strip() for x in date_cols]

    # 제품명
    products = dom.select('#divProduct > div.ul_col2_l > div > div.um_table.pd_t1 > table > tbody > tr > th')
    products = [x.text.replace('\xa0',' ').strip() for x in products]
    df = pd.DataFrame(products)

    # 제품별 매출비중
    data_values = dom.select('#divProduct > div.ul_col2_l > div > div.um_table.pd_t1 > table > tbody > tr > td')
    data_values = [x.text.strip() for x in data_values]

    try:
        # 데이터 df
        lap = len(date_cols) -1
        for i in range(lap):
            temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
            df = pd.concat([df, temp_df], axis=1)
        df.columns=date_cols
        df = df[df.columns.tolist()[::lap]]
        # 마지막 정리
        # 본래 날짜 컬럼인것을 '구성비'로 수정하고 마지막 날짜만 남기기
        mix_date = df.columns.tolist()[-1] # 마지막 날짜 컬럼명
        df.rename(columns={mix_date:'구성비'}, inplace=True)
        df['기준날짜'] = mix_date # 마지막날짜를 기준날짜로 삽입
        # 길이가 0이 안되면 삭제
        filt = df['구성비'].apply(len) > 0
        df = df.loc[filt].copy()
        df['구성비'] = df['구성비'].astype('float')/100
        # 제품명이 기타(계)인 것 보다 아래에 있으면 삭제
        filt = df['제품명']=='기타(계)'
        try:
            df = df[df.index.tolist() < df[filt].index].copy().reset_index(drop=True)
        except:
            pass
        # 결과물 추가
        df['종목코드'] = code
        sales_mix = sales_mix.append(df)
        del df, filt, mix_date, data_values, products, date_cols
    except:
        pass

    #----- 시장점유율 가장 최근 ------ ------ ------ ------ ------ ------ ------ ------ ------
    # 표 컬럼
    ms_col = dom.select('#divProduct > div.ul_col2_r > div > div.um_table.pd_t1 > table > thead > th')
    ms_col = [x.text.strip() for x in ms_col]
    # 제품 컬럼
    products = dom.select('#divProduct > div.ul_col2_r > div > div.um_table.pd_t1 > table > tbody > tr > th')
    products = [x.text.replace('\xa0',' ').strip() for x in products]
    # 제품별 시장점유율
    data_values = dom.select('#divProduct > div.ul_col2_r > div > div.um_table.pd_t1 > table > tbody > tr > td')
    data_values = [x.text.strip() for x in data_values]
    # 한번에 df로 만들기
    df = pd.DataFrame([products, data_values]).T
    try:
        df.columns = ms_col
        # 마지막 정리
        filt1 = df['시장점유율'].apply(len) > 0
        filt2 = df['주요제품']!='전체'
        filt = filt1 & filt2
        df = df.loc[filt].copy()
        df['시장점유율'] = df['시장점유율'].astype('float')/100
        df['종목코드'] = code
    # 결과물 추가
        market_share = market_share.append(df)
        del filt1, filt2, df, data_values, products, ms_col
    except:
        pass

    #----- 판관비율추이, 매출원가율추이 ------ ------ ------ ------ ------ ------ ------ ------
    cost_types = {'panguanD_01':'연결,판관비율', 'panguanB_01':'별도,판관비율',
                    'panguanD_02':'연결,매출원가율', 'panguanB_02':'별도,매출원가율'}
    df = pd.DataFrame()
    for cost_type in cost_types.keys():
        try:
            date_cols = dom.select(f'#{cost_type} > div > div.um_table > table > thead > tr > th')
            date_cols = [x.text.strip() for x in date_cols]
            data_values = dom.select(f'#{cost_type} > div > div.um_table > table > tbody > tr > td')
            data_values = [cost_types[cost_type]] +  [x.text.strip() for x in data_values]
            temp = pd.DataFrame(data_values).T
            temp.columns = date_cols
            df = df.append(temp)
        except:
            pass
    try:
        df = df.melt(id_vars='항목',var_name='날짜',value_name='값')
        df = clean_numeric_value(df)
        df['값'] = df['값']/100
        df['연결/별도'] = df['항목'].str.split(',', expand=True)[0]
        df['항목'] = df['항목'].str.split(',', expand=True)[1]
        df['종목코드'] = code
        # 결과물 추가
        cogs_n_oc = cogs_n_oc.append(df)
        del df, date_cols, data_values, temp
    except:
        del df, date_cols, data_values, temp

    # 수출 및 내수구성 정리
    corp_types = {'corpExport_D':'연결',
                  'corpExport_B':'별도'}
    df = pd.DataFrame()
    for corp_type in corp_types.keys():
        try:
            # 날짜컬럼
            col = dom.select(f'#{corp_type} > table > thead > tr.th2row_f > th')
            col = [x.text.strip() for x in col]
            # ['매출유형', '제품명', '2017/12', '2018/12', '2019/12']
            #col의 결과에서 날짜만 선택 → ['2017/12', '2018/12', '2019/12']
            date_cols = col[2:]
            # 날짜와 내수/수출 combination된 df 만들기
            date_df = pd.DataFrame(['매출유형','제품명'] + [(x,y) for x in date_cols for y in ['내수','수출']])
            #------ ------ ------ ------ ------ ------ ------ ------
            # 매출유형 철럼
            sales_type = dom.select(f'#{corp_type} > table > tbody > tr > td.clf') # 매출유형
            sales_type = [x.text.strip() for x in sales_type]
            # ['제품', '기타', '기타', '']
            #------ ------ ------ ------ ------ ------ ------ ------
            # 제품명
            products = dom.select(f'#{corp_type} > table > tbody > tr > td.l')
            products =  [x.text.replace('\xa0',' ').strip()  for x in products]
            # ['Display 패널', 'LCD, OLED 기술 특허', '원재료,부품 등', '합계']
            # 매출유형+제품명 합친 df
            col_df = pd.DataFrame([sales_type, products]).T
            #------ ------ ------ ------ ------ ------ ------ ------
            # 데이터 정리
            data_values = dom.select(f'#{corp_type} > table > tbody > tr > td.r')
            data_values = [x.text.strip() for x in data_values]
            lap = len(date_cols)*2 # 다중index 컬럼이라서 *2 (날짜별 내수/수출)
            temp_df = pd.DataFrame()
            for i in range(lap):
                temp = pd.DataFrame(data_values[i::lap])
                temp_df = pd.concat([temp_df, temp], axis=1)
            #------ ------ ------ ------ ------ ------ ------ ------
            # 매출유형+제품명 합친 df + 데이터 df
            temp_df = pd.concat([col_df, temp_df], axis=1)
            # 매출유형+제품명 합친 df + 데이터 df + 날짜df
            temp_df = pd.concat([date_df, temp_df.T.reset_index(drop=True)], axis=1).T
            #------ ------ ------ ------ ------ ------ ------ ------
            # 합쳐진거 전처리
            # 첫행을 column으로 셋팅하고 첫행 삭제
            temp_df.columns = temp_df.iloc[0]
            temp_df = temp_df.iloc[1:,].copy()
            # wide to tidy
            temp_df = temp_df.melt(id_vars=['매출유형','제품명'], var_name='임시', value_name='값')
            temp_df['연결/별도'] = corp_types[corp_type]
            # 컬럼 정리
            df = df.append(temp_df)
        except:
            pass
    try:
        df['날짜'] = df['임시'].str[0]
        df['수출/내수'] = df['임시'].str[1]
        df.loc[df['매출유형'].apply(len)==0,'매출유형'] = df.loc[df['매출유형'].apply(len)==0,'제품명']
        col = ['매출유형','제품명','수출/내수','연결/별도','날짜','값']
        df = df[col].sort_values(by='날짜', ascending=False)
        '''
        값이 없는 경우에도 제품명은 가져오기 위해서
        df['값'].sum() ==''으로 test해서 각 경우 별도로 처리
        '''
        df = df.drop_duplicates(subset=['매출유형','제품명','연결/별도','값'])
        if df['값'].sum() =='':
            df = df.drop_duplicates(subset=['매출유형','연결/별도','제품명'])
            df['값'] = 0
        else:
            filt = df['값'].apply(len)>0
            df = df.loc[filt].copy()
            df['값'] = df['값'].astype('float')/100
        df['종목코드'] = code
        df.reset_index(drop=True, inplace=True)
        # 결과물 추가
        export_n_domestic = export_n_domestic.append(df)
        del df, col, temp_df, data_values, products, date_cols
    except:
        pass
    #--------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_fnguide_company_info(param='all'):
    start_time = helper.now_time()
    # fnguide 기업정보 결과물 넣을 dataframes---------------
    global company_info  # 업종, business summary
    global financial_highlights  # 재무제표
    global sales_mix  # 제품별 매출비중 가장 최근
    global market_share  # 시장점유율 가장 최근
    global cogs_n_oc  # 판관비율추이, 매출원가율추이
    global export_n_domestic  # 수출 및 내수 구성
    global code_list
    print('FNguide 기업정보 가져오기 시작 ' + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     executor.map(get_fnguide_company_info, code_list)
    if param !='all':
        new = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
        old = conn_db.from_("DB_기업정보", 'from_fnguide_기업정보')['종목코드']
        new_code_list = list(set(new) - set(old))
        if len(new_code_list)>0:
            dummy = [get_fnguide_company_info(code) for code in new_code_list]
        else:
            print('업데이트할 내역 없음')
            del new, old, new_code_list
    else:
        dummy = [get_fnguide_company_info(code) for code in code_list]
    if len(dummy)>0:
        del dummy

    # 업종, business summary------- ------- ------- ------- ------- ------- ------- -------
    if len(company_info)>0:
        company_info = helper.make_keycode(company_info.reset_index(drop=True))
        # 코스피(KSE)와 코스닥(KOSDAQ)이 별도 컬럼에 있는데 KRX하나로 통합
        all_cols = company_info.columns.tolist()
        for col in all_cols: # 컬럼별로 앞뒤 공백제거
            company_info[col] = company_info[col].str.strip()
        if 'KSE' in all_cols:
            company_info['KRX'] = (company_info['KSE'].astype(str) + company_info['KOSDAQ'].astype(str)).str.replace('nan','').str.strip()
        else:
            company_info['KRX'] = company_info['KOSDAQ'].astype(str).str.replace('nan','').str.strip()
        company_info['KRX'] = company_info['KRX'].str.split(' ',1,expand=True)[1] # 종목앞에 코스피/코스닥이 있어서 삭제
        company_info['KRX'].fillna(company_info['FICS'], inplace=True)
        if 'KSE' in all_cols:
            company_info = company_info.rename(columns={0:'기준날짜',  1:'요약', 2:'내용'}).drop(columns=['KSE','KOSDAQ'])
        else:
            company_info = company_info.rename(columns={0:'기준날짜',  1:'요약', 2:'내용'}).drop(columns='KOSDAQ')
        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        old_df = conn_db.from_('DB_기업정보','from_fnguide_기업정보')
        cols = ['KEY']
        company_info = drop_duplicate_rows(company_info, old_df, cols)

        try:
            company_info.rename(columns={'et':'기준날짜'},inplace=True)
        except:
            pass

        conn_db.to_(company_info, 'DB_기업정보', 'from_fnguide_기업정보')
        # 취합본 수정해 놓기
        import co_info as co
        co.get_all_co_info()
        del company_info, old_df

    # 재무제표------- ------- ------- ------- ------- ------- ------- -------
    if len(financial_highlights)>0:
        financial_highlights = helper.make_keycode(financial_highlights.reset_index(drop=True))
        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        file = folder_fn_backup + "fnguide_financial_highlights_원본취합본.pkl"
        old_df = pd.read_pickle(file)
        # 컬럼 = 항목, 연결/별도, 연간/분기, 날짜, 값, 종목코드, 종목명, KEY
        cols = ['항목', '연결/별도', '날짜', '연간/분기', '종목코드', '종목명', 'KEY']
        df = drop_duplicate_rows(financial_highlights, old_df, cols)
        df.to_pickle(file)

        # 전처리------------------------------------------------------
        for item in [col for col in df['항목'].unique().tolist() if '%' in col]:
            temp = df.loc[df['항목'] == item, '값']
            df.loc[df['항목']==item, '값'] = temp/100

        try:
            dates = df.loc[df['날짜'].str.contains('(P)'), ['날짜']]
            dates['날짜'] = dates['날짜'].str.split('\n', expand=True).iloc[:,-1:]
            df.loc[df['날짜'].str.contains('(P)'), ['날짜']] = dates
        except:
            pass

        # 실적/전망 컬럼 생성
        for expect in ['E','P']:
            if df['날짜'].str.contains(expect).sum()>0:
                df['실적/전망'] = df['날짜'].apply(lambda x : '전망' if expect in x else '실적')
                df['날짜'] = df['날짜'].str.replace(f'\({expect}\)', '')

        # 항목에 있는 괄호 부분 삭제
        for item in ['\(원\)', '\(배\)','\(%\)']:
            df['항목'] = df['항목'].str.replace(item, '')

        # 주재무제표만 선택--------------------------------------------------
        df.drop(columns=['종목명','종목코드'], inplace=True)
        cols = df.columns.tolist()
        df['temp_key'] = df['KEY']+df['연결/별도']
        main_df = conn_db.from_('DB_기업정보','종목별_주재무제표')
        main_df['재무제표기준'] = main_df['재무제표기준'].str.replace('IFRS','')
        main_df['temp_key'] = main_df['KEY']+main_df['재무제표기준']
        filt = df['temp_key'].isin(main_df['temp_key'])
        df = df.loc[filt,cols].copy()
        df = df.drop(columns=['연결/별도']).reset_index(drop=True)

        # 저장하기---------------------------------------------------
        df.to_pickle(conn_db.get_path('fnguide_financial_highlights') + '.pkl')
        cols = ['KEY','연간/분기','날짜','실적/전망']
        df = df.pivot_table(index=cols, columns='항목', values='값').reset_index()
        df.columns.name=None
        conn_db.to_(df,'fnguide_fs_highlights','fs')

    # 제품별 매출비중 가장 최근------- ------- ------- ------- ------- ------- ------- -------
    if len(sales_mix)>0:
        sales_mix = helper.make_keycode(sales_mix.reset_index(drop=True))

        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        old_df = conn_db.from_('DB_기업정보', 'sales_mix_from_fnguide')
        cols = ['종목코드', '종목명', '제품명', 'KEY']
        sales_mix = drop_duplicate_rows(sales_mix, old_df, cols)

        # 컬럼 = 제품명,구성비,기준날짜,종목코드,종목명,KEY
        conn_db.to_(sales_mix, 'DB_기업정보', 'sales_mix_from_fnguide')
        del sales_mix, old_df

    # 시장점유율 가장 최근------- ------- ------- ------- ------- ------- ------- -------
    if len(market_share):
        market_share = helper.make_keycode(market_share.reset_index(drop=True))
        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        old_df = conn_db.from_('DB_기업정보', 'mkt_share_from_fnguide')
        # 컬럼 = 주요제품,시장점유율,종목코드,종목명,KEY
        cols = ['종목코드', '종목명', '주요제품', 'KEY']
        market_share = drop_duplicate_rows(market_share, old_df, cols)
        conn_db.to_(market_share, 'DB_기업정보', 'mkt_share_from_fnguide')
        del market_share, old_df

    # 판관비율추이, 매출원가율추이------- ------- ------- ------- ------- ------- ------- -------
    if len(cogs_n_oc)>0:
        cogs_n_oc = cogs_n_oc.pivot_table(index=['날짜','연결/별도','종목코드'],
                                        columns='항목',values='값').reset_index()
        cogs_n_oc.columns.name = None
        cogs_n_oc = helper.make_keycode(cogs_n_oc.reset_index(drop=True))
        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        file = folder_fn_backup + 'fnguide_판관비율매출원가율_원본취합본.pkl'
        old_df = pd.read_pickle(file)

        # 컬럼 = 날짜, 연결/별도, 종목코드, 매출원가율, 판관비율, 종목명, KEY
        cols = ['날짜', '연결/별도', 'KEY', '종목코드','종목명']
        cogs_n_oc = drop_duplicate_rows(cogs_n_oc, old_df, cols)
        cogs_n_oc.to_pickle(file)

        cogs_n_oc['날짜'] = cogs_n_oc['날짜'].apply(lambda x : x.replace('/','-') if '/' in x else x.replace('.','-') if '.'in x else x )
        cogs_n_oc['key'] = cogs_n_oc['연결/별도']+cogs_n_oc['KEY']
        main_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
        main_fs['재무제표기준'] = main_fs['재무제표기준'].str[-2:]
        main_fs['key'] = main_fs['재무제표기준']+main_fs['KEY']
        cogs_n_oc = main_fs.merge(cogs_n_oc, on=['key', 'KEY'], how='inner').drop(columns='key')
        cols = ['KEY','재무제표기준','날짜','매출원가율','판관비율']
        cogs_n_oc = cogs_n_oc[cols].reset_index(drop=True)
        conn_db.to_(cogs_n_oc, 'DB_기업정보', '매출원가율_판관비율_from_fnguide')
        del cogs_n_oc, old_df

    # 수출 및 내수 구성------- ------- ------- ------- ------- ------- ------- -------
    if len(export_n_domestic)>0:
        export_n_domestic = export_n_domestic.pivot_table(index=['날짜','연결/별도','종목코드','매출유형','제품명'],
                                                        columns='수출/내수',values='값').reset_index()
        export_n_domestic.columns.name=None
        export_n_domestic = export_n_domestic.sort_values(by=['종목코드','날짜',
                                                            '연결/별도','매출유형']).reset_index(drop=True)
        export_n_domestic = helper.make_keycode(export_n_domestic.reset_index(drop=True))
        export_n_domestic['날짜'] = export_n_domestic['날짜'].apply(lambda x: x.replace('/', '-') if '/' in x else x.replace('.', '-') if '.'in x else x)

        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        file = folder_fn_backup+'제품별_수출및내수_구성비_받은원본취합본.pkl'
        old_df = pd.read_pickle(file)

        # 컬럼 = 날짜,연결/별도,종목코드,매출유형,제품명,내수,수출,종목명,KEY
        cols = ['날짜', '연결/별도','매출유형','KEY', '제품명','종목코드','종목명']
        export_n_domestic = drop_duplicate_rows(export_n_domestic, old_df, cols)
        export_n_domestic.to_pickle(file) # 백업

        # 주재무제표만 필터링하고 저장
        export_n_domestic['key'] = export_n_domestic['연결/별도']+export_n_domestic['KEY']
        main_fs = conn_db.from_('DB_기업정보','종목별_주재무제표')
        main_fs['재무제표기준'] = main_fs['재무제표기준'].str[-2:]
        main_fs['key'] = main_fs['재무제표기준']+main_fs['KEY']
        export_n_domestic = main_fs.merge(export_n_domestic,
                                            on=['key', 'KEY'], how='inner').drop(columns='key')
        cols = ['KEY','재무제표기준','날짜','매출유형','제품명','내수','수출']
        export_n_domestic = export_n_domestic[cols].reset_index(drop=True)
        conn_db.to_(export_n_domestic,'DB_기업정보','export_n_domestic_from_fnguide')

    #------- ------- -------
    print('FNguide 기업정보 가져오기 완료 ' + helper.now_time().strftime('%Y-%m-%d %H:%M:%S'))
    print('소요시간: ' + str(helper.now_time() - start_time))

#네이버 재무제표
def get_fs_from_naver(code):  # 네이버증권에서 종목의 기업실적분석 표 가져오기
    '''
    네이버증권에서 종목의 기업실적분석 표 가져오기
    '''
    url = f'https://finance.naver.com/item/main.nhn?code={code}'
    r = requests.get(url, headers={'User-Agent': user_agent})
    time.sleep(1)
    dom = BeautifulSoup(r.content, "lxml")
    try:
        # 컬럼 header가 들어 있는 변수
        selector = '#content > div.section.cop_analysis > div.sub_section > table > thead > tr > th'
        col_header = dom.select(selector)
        # data가 들어 있는 변수
        selector = '#content > div.section.cop_analysis > div.sub_section > table > tbody > tr'
        data_by_accounts = dom.select(selector)
        df = pd.DataFrame()  # 취합할 dataframe
        # 1. 계정별 data 가져오기
        for i in range(len(data_by_accounts)):
            data = pd.DataFrame([data.text.strip() for data in data_by_accounts[i].select('td')])
            df = pd.concat([df, data], axis=1)
        # 2. 계정 명칭 가져오기
        header_info = []
        for i in range(len(data_by_accounts)):
            header = data_by_accounts[i].select('th')[0].text.strip()
            header_info.append(header)
        df.columns = header_info
        # 3. 날짜 컬럼 가져오기
        all_col = [data.text.strip() for data in col_header[3:]]  # 앞부분은 불필요한 컬럼 정보가 있어서 제외
        estimate_dates = [x for x in all_col if '(E)' in x]  # 예상치가 있는 컬럼
        # 연간 컬럼 날짜 정리
        y = all_col.index(estimate_dates[0])+1  # 연간실적 컬럼이 끝나는 위치
        y_date = all_col[:y]
        dummy = [all_col.remove(date) for date in y_date]
        y_date = [date+'_연간' for date in y_date]
        # 분기 컬럼 날짜 정리
        q = all_col.index(estimate_dates[1])+1  # 분기실적 컬럼이 끝나는 위치
        q_date = all_col[:q]
        dummy = [all_col.remove(date) for date in q_date]
        q_date = [date+'_분기' for date in q_date]
        # 날짜 컬럼
        date_col = pd.DataFrame(y_date + q_date).rename(columns={0: '날짜'})
        # 재무제표 기준 컬럼
        fstype_col = pd.DataFrame(all_col).rename(columns={0: '재무제표기준'})
        df = pd.concat([date_col, fstype_col, df], axis=1)
        df = df.melt(id_vars=['날짜', '재무제표기준'], var_name='항목', value_name='값')
        df[['날짜', '주기']] = df['날짜'].str.split('_', expand=True)
        # 날짜 컬럼 값에 'E'가 있으면 전망치임. 별도 컬럼을 만든 다음에 날짜컬럼에서는 삭제
        df['실적/전망'] = ['전망' if 'E' in x else '실적' for x in df['날짜']]
        df['날짜'] = df['날짜'].str.replace('\(E\)', '')
        df['종목코드'] = code
        return df
    except:
        print(f'{code} 데이터가져오기 실패')
        pass

@helper.timer
def update_naver_fs(param='all'): # 네이버증권 기업실적분석표 업데이트
    global code_list
    start_time = helper.now_time()
    print('네이버 재무재표 가져오기 시작 ' + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    if param =='all':
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            result = executor.map(get_fs_from_naver, code_list)
            df = pd.concat([df for df in result], axis=0)
    else:
        new = conn_db.from_("DB_기업정보", 'FS_update_list')[['KEY','종목코드']]
        old_df = conn_db.from_("from_naver증권", 'naver_최근값만')[['KEY']].drop_duplicates()
        code_list_added = pd.DataFrame(list(set(new['KEY']) - set(old_df)))
        code_list_added = code_list_added.merge(new, left_on=0, right_on='KEY')['종목코드']
        df = pd.concat([get_fs_from_naver(code) for code in code_list_added], axis=0)
        df.reset_index(drop=True, inplace=True)

    if len(df)>0:
        #새로 가져온 것 전처리----------------
        df = clean_numeric_value(df)
        dcols = set(df.columns.tolist()) - set(['항목','값',])
        df = df.pivot_table(index = dcols, columns = '항목', values='값',).reset_index()
        df.columns.name = None

        # 취합본에 합혀서 저장----------------
        file = folder_naver_backup + "fs_from_naver_원본_취합본.pkl"
        old_df = pd.read_pickle(file)

        cols = ['날짜', '재무제표기준', '종목코드', '주기']
        df = drop_duplicate_rows(df, old_df, cols)
        df.to_pickle(file) # conn_db.to_(df, 'from_naver증권', '원본_취합본') 대체
        del old_df
        print('네이버증권 기업실적분석표 가져오기 완료')
        print('(소요시간: ' + str(helper.now_time() - start_time)+")")

        # 취합본 불러와서 공통부분 전처리
        # DB_기업정보 FS_update_list에 있는 코드만 필터링하고 KEY컬럼 만들기
        df = df[df['종목코드'].isin(code_list)].copy()
        df = helper.make_keycode(df).drop(columns=['종목명','종목코드'])

        # 비율인 컬럼 100으로 나눠주기
        matcher = ['ROE', '률', '율', '배당성향']
        all_cols = df.columns.tolist()
        prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
        for col in prcnt_cols:
            df[col] = df[col]/100
        # 행 정렬
        df = df.sort_values(by=['KEY','날짜', '주기'], ascending=False)

        #--------기업정보랑 합쳐서 저장 ------- ------- ------- ------- ------- ------- -------
        df_all = df.merge(conn_db.from_("DB_기업정보", '취합본'), on='KEY', how='left')
        # 새로 합쳐진 것 저장_시계열용
        conn_db.to_(df_all, 'from_naver증권', 'naver_final')
        df_all.to_pickle(folder_naver + "fs_from_naver_final.pkl")

        print('네이버증권 기업실적분석표+기업정보취합본 merge후 pickle 저장완료')

        #회사별 주재무제표만 정리----------------------------
        temp = df[['재무제표기준','KEY']].drop_duplicates(subset=['KEY']).reset_index(drop=True)
        conn_db.to_(temp,'DB_기업정보','종목별_주재무제표')

        del df_all, temp

        #--------최근 연간/분기 실적/전망만 정리 ------- ------- ------- ------- ------- -------
        # 컬럼명에 '(' 부분 삭제
        name = {col: col.split('(')[0]}
        [df.rename(columns=name, inplace=True) for col in df.columns.tolist() if "("in col]

        filt_q = df['주기'] != '연간'
        filt_y = df['주기'] == '연간'
        filt_expect = df['실적/전망'] != '실적'
        filt_real = df['실적/전망'] == '실적'

        # 사용안하는 컬럼 삭제
        cols = ['부채비율','당좌비율','유보율','시가배당률','배당성향']
        df.drop(columns=cols, inplace=True)
        # 컬럼 뒤에 추가할 suffix 정의
        expect_suffix = ' (E)' # 전망인 경우 컬럼 뒤에 추가
        y_suffix = '_Y' # 연간인 경우 컬럼 뒤에 추가
        ly_suffix = '_LY' # 전년 연간의 경우 컬럼 뒤에 추가
        q_suffix = '_Q' # 분기인 경우 컬럼 뒤에 추가
        lq_suffix = '_LQ' # 전분기인 경우 컬럼 뒤에 추가

        #----- ----- ----- ----- ----- ----- ----- ----- ----- -----
        def get_last_row(df):  # 주기, 실적/전망 별로 전처리 하는 함수
            # 종목별로 가장 최근 날짜만 남기기
            df = df.groupby(['KEY'], as_index=False).head(1).copy()
            values_cols = df.columns.tolist()[4:-1] # 측정값만 있는 컬럼 list
            # 주기에 따라 컬럼명 뒤에 _Y / _Q 추가
            if df['주기'].unique().tolist()[0] == '연간':
                [df.rename(columns={col: col + y_suffix}, inplace=True) for col in values_cols]
            else:  # 분기의 경우 주당배당금이 없어서 삭제
                [df.drop(columns=[col], inplace=True) for col in df.columns.tolist() if "주당배당금"in col]
                [df.rename(columns={col: col + q_suffix}, inplace=True) for col in values_cols]

            # 위에서 컬럼명이 수정되었기 때문에 value_cols다시 정의
            values_cols = df.columns.tolist()[4:-1]
            # '전망'인 경우 컬럼명 뒤에 '(E)' 추가
            if df['실적/전망'].unique().tolist()[0] == '전망':
                [df.rename(columns={col: col + expect_suffix}, inplace=True) for col in values_cols]
            # 측정값 이외의 컬럼 삭제
            df.drop(columns=['실적/전망', '날짜', '재무제표기준', '주기'], inplace=True)
            return df.reset_index(drop=True)

        #----- ----- ----- ----- ----- ----- ----- ----- ----- -----
        def get_prior_row(df): # 전년도 연간/분기 실적 전처리 하는 함수
            # 올해 - KEY를 기준으로 가장 위에만 keep
            df_this = df.groupby(['KEY'], as_index=False).head(1)
            # 전년 - KEY를 기준으로 가장 위에2개만 keep한 다음에 다시 KEY를 기준으로 가장 마지막 keep
            df_last = df.groupby(['KEY'], as_index=False).head(2).groupby(['KEY'], as_index=False).tail(1)
            # 전년도 실적이 없는 경우가 있음.
            # 그래서 올해 실적 df를 concat할때 2번 넣어서 중복삭제로 모두 없앤다
            df = pd.concat([df_last, df_this, df_this]).drop_duplicates(keep=False)
            del df_last, df_this

            values_cols = df.columns.tolist()[4:-1]  # 측정값만 있는 컬럼 list
            if df['주기'].unique().tolist()[0] == '연간':
                [df.rename(columns={col: col + ly_suffix}, inplace=True) for col in values_cols]
            else: # 분기의 경우 주당배당금이 없어서 삭제
                [df.drop(columns=[col], inplace=True) for col in df.columns.tolist() if "주당배당금"in col]
                [df.rename(columns={col: col + lq_suffix}, inplace=True) for col in values_cols]
            # 측정값 이외의 컬럼 삭제
            df.drop(columns=['실적/전망', '날짜', '재무제표기준', '주기'], inplace=True)
            return df.reset_index(drop=True)

        # 연간 df
        df_y_expect = get_last_row(df.loc[filt_y & filt_expect].copy()) # 연간 전망
        df_y_real = get_last_row(df.loc[filt_y & filt_real].copy()) # 연간 실적
        df_y_real_last = get_prior_row(df.loc[filt_y & filt_real].copy()) # 전년 실적
        df_y = df_y_real.merge(df_y_real_last, on='KEY', how='left').merge(df_y_expect, on='KEY', how='left')
        del df_y_expect, df_y_real, df_y_real_last, filt_y

        # 분기 df
        df_q_expect = get_last_row(df.loc[filt_q & filt_expect].copy()) # 분기 전망
        df_q_real = get_last_row(df.loc[filt_q & filt_real].copy()) # 분기 실적
        df_q_real_last = get_prior_row(df.loc[filt_q & filt_real].copy()) # 분기 실적
        df_q = df_q_real.merge(df_q_real_last, on='KEY', how='left').merge(df_q_expect, on='KEY', how='left')
        del df_q_expect, df_q_real, df_q_real_last, filt_q, filt_expect, filt_real

        # 전체 합치기 + 'KEY'컬럼을 맨 앞으로 한 다음 저장
        df_all = df_y.merge(df_q, on='KEY', how='outer')
        cols = ['KEY'] + [col for col in df_all if col != 'KEY']
        df_all.dropna(axis=1, how='all', inplace=True)

        #--------------------------------------------------------------------
        conn_db.to_(df_all, 'from_naver증권', 'naver_최근값만')
        df_all[cols].to_pickle(folder_naver + "fs_from_naver_최근값만.pkl")
        print('네이버증권 최근값만 저장 완료')
        merge_df_all_numbers() # 전체 취합본 업데이트

    else: #새로 가져온 df가 값이 없을때
        print('업데이트할 내용 없음')
#--------------------------------------------------------------------------------------------------------------------
# 아이투자 투자지표
def get_table_from_itooza(param): # 5개년 주요 투자지표 및 최근것까지 반영된 투자지표 가져오기
    global df_tables
    global df_5yr
    global df_short

    code, co = param
    url = f'http://search.itooza.com/search.htm?seName={code}'
    r = requests.get(url, headers={'User-Agent': user_agent})
    # r.encoding = r.apparent_encoding # 원래 이거였는데 encoding방식이 변경됨?
    r.encoding ='euc-kr'
    time.sleep(1)
    dom = BeautifulSoup(r.text, "html.parser")
    #-------------------------------------------------------------------------------
    # 페이지 아래쪽에 있는 테이블3개
    report_types = ['indexTable1', 'indexTable2', 'indexTable3']
    report_type_name= {'indexTable1':'연환산',
                        'indexTable2':'연간',
                        'indexTable3' : '분기'}
    df_all = pd.DataFrame()
    for report_type in report_types:
        try:
            data_table = dom.select(f'#{report_type} > table')[0]
            # 지표 header
            item_col = data_table.select('tbody > tr > th')
            df = pd.DataFrame([x.text.strip() for x in item_col])
            # 날짜 header
            date_cols = data_table.select('thead > tr > th')
            date_cols = [x.text.strip() for x in date_cols]
            date_cols = [data.replace('.','년') for data in date_cols]
            # 데이터 값
            data_values = data_table.select('tbody > tr > td')
            data_values = [x.text.strip() for x in data_values]

            lap = len(date_cols)-1
            for i in range(lap):
                temp_df = pd.DataFrame(data_values[i::lap]) # 컬럼의 갯수(lap)만큼 매n번째마다 합쳐줌
                df = pd.concat([df, temp_df], axis=1)
            df.columns = date_cols
            df = df.melt(id_vars='투자지표', var_name='날짜', value_name='값')
            df = df.loc[~(df['값']=='N/A')]
            df.rename(columns={'투자지표':'항목'}, inplace=True)
            df.insert(loc=0, column='기준', value=report_type_name[report_type])
            df_all = df_all.append(df)
        except:
            pass
    if len(df_all)>0:
        df_all['종목코드'] = code
        df_tables = df_tables.append(df_all)
        del df_all, df
    else:
        print(f'{param} 전체 테이블 가져오기 실패')

    #-------------------------------------------------------------------------------
    # 5년평균
    try:
        all_data = dom.select('#stockItem > div.item-body > div.ar > div.item-data2')[0]
        df_fiveyr = pd.DataFrame([x.text.strip() for x in all_data.select('td')]).T
        headers = [x.text.strip() for x in all_data.select('th')]
        df_fiveyr.columns = headers
        df_fiveyr.insert(loc=0, column='종목명', value=co)
        df_fiveyr.insert(loc=0, column='종목코드', value=code)
        # memo = dom.select('#stockItem > div.item-body > div.ar > div.item-data2 > p')[0].text.split('* ')[1]
        # df_fiveyr['비고'] = memo
        for col in df_fiveyr.columns.tolist():
            df_fiveyr[col] = df_fiveyr[col].str.replace('\(-\)','')
            df_fiveyr[col] = df_fiveyr[col].str.replace('N/A','')
        df_5yr = df_5yr.append(df_fiveyr)
        del df_fiveyr
    except:
        print(f'{param} 5개년 지표 가져오기 실패')

    #-------------------------------------------------------------------------------
    # 최근 지표 가져오기
    try:
        all_data = dom.select('#stockItem > div.item-body > div.ar > div.item-data1')[0]
        df = pd.DataFrame([x.text.strip() for x in all_data.select('td')]).T
        headers = [x.text.strip() for x in all_data.select('th')]
        df.columns = headers
        fix_header = 'ROE = ROS * S/A * A/E'
        roe_all = df[fix_header].tolist()[0]
        #---------------------------
        # A/E
        AE = roe_all.split('=')[1].split('*')[2].strip('(')[:-1]
        df.insert(loc=2, column='A/E',value= AE)
        # S/A
        SA = roe_all.split('=')[1].split('*')[1].strip('(')[:-1]
        df.insert(loc=2, column='S/A',value= SA)
        # ROS
        ROS = roe_all.split('=')[1].split('*')[0].strip('(')[:-1]
        df.insert(loc=2, column='ROS',value= ROS)
        # ROE
        ROE = roe_all.split('=')[0]# ROE
        df.insert(loc=2, column='ROE', value=ROE)
        #---------------------------
        df.drop(columns=fix_header, inplace=True)
        df.insert(loc=0, column='종목명', value=co)
        df.insert(loc=0, column='종목코드', value=code)
        for col in df.columns.tolist():
            df[col] = df[col].str.replace('\(-\)','')
            df[col] = df[col].str.replace('N/A','')
        # memo = dom.select('#stockItem > div.item-body > div.ar > div.item-detail > p.table-desc')[0].text.split('* ')[1]
        # df['비고'] = memo
        df_short = df_short.append(df)
        del df, all_data
    except:
        print(f'{param} 최근 지표 가져오기 실패')
    del dom
    # 5개년 주요 투자지표 alc 전체 투자지표 업데이트 -------------------------------------------------------------------

@helper.timer
def update_itooza_fsratio(param='all'): # 5개년 주요 투자지표 업데이트
    #itooza 투자지표 업데이트
    global code_list
    global max_workers
    global df_tables
    global df_5yr
    global df_short
    start_time = helper.now_time()
    print('아이투자 투자지표 가져오기 시작 ' + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    #--------- --------- --------- --------- --------- --------- --------- --------- --------- ---------
    # 아이투자 투자지표 업데이트
    if param !='all': # 추가분만 업데이트할 때, 업데이트할 내역이 있는지 확인
        new = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
        old = conn_db.from_("DB_기업정보", 'from_아이투자_기업정보')['종목코드'].unique().tolist()
        new_code = list(set(new) - set(old))
        if len(new_code)>0:
            new = conn_db.from_("DB_기업정보", 'FS_update_list')[['종목코드','종목명']]
            code_list_temp = new[new['종목코드'].isin(new_code)].reset_index(drop=True)
            code_list_temp = code_list_temp.values.tolist()
            [get_table_from_itooza(param) for param in code_list_temp]
            print('(가져오기 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
            del code_list_temp
        else:
            print('업데이트할 내역 없음')
        del new, old, new_code

    else: # 전체 업데이트
        code_list_temp = conn_db.from_("DB_기업정보", 'FS_update_list')[['종목코드','종목명']]
        code_list_temp = code_list_temp.values.tolist()
        [get_table_from_itooza(param) for param in code_list_temp]
        # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        #     executor.map(get_table_from_itooza, code_list_temp)
        print('(1차 가져오기 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
        print('누락된거 확인후 다시 시도')
        done_code = df_tables['종목코드'].unique().tolist()
        new = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
        new_code = list(set(new) - set(done_code))
        if len(new_code)>0:
            new = conn_db.from_("DB_기업정보", 'FS_update_list')[['종목코드', '종목명']]
            code_list_temp = new[new['종목코드'].isin(new_code)].reset_index(drop=True)
            code_list_temp = code_list_temp.values.tolist()
            [get_table_from_itooza(param) for param in code_list_temp]
            del code_list_temp, new, new_code, done_code
        else:
            print('누락된거 없음. 전처리 시작')
            pass

    #----------- ----------- ----------- ----------- ----------- ----------- ----------- ----------- -----------
    # 테이블에 있는 지표 정리
    if len(df_tables)>0:
        df = df_tables
        df = clean_numeric_value(df)
        # 지표를 컬럼으로 행/열전환
        dcols = set(df.columns.tolist()) - set(['항목', '값'])
        df = df.pivot_table(index=dcols, columns='항목', values='값').reset_index()
        df.columns.name = None
        # 컬럼명 간단하게 수정 (뒤에 '(%)' 가 있는 경우 삭제하기 위함)
        df.columns = [x.split(' ')[0] for x in df.columns.tolist()]
        #----------- ----------- ----------- ----------- ----------- ----------- ----------- ----------- -----------
        # 취합본과 합쳐놓기
        # file = folder_itooza_backup + '0_아이투자_시계열_원본취합본.pkl'
        # df_old = pd.read_pickle(file)
        df_old = conn_db.from_('from_아이투자','아이투자_시계열_원본취합본')
        df = pd.concat([df, df_old], axis=0)
        cols = ['기준', '종목코드', '날짜']
        df = df.drop_duplicates(cols).reset_index(drop=True)
        # 새로 합쳐진 것 저장
        conn_db.to_(df,'from_아이투자','아이투자_시계열_원본취합본')
        # df.to_pickle(file)
        del df_tables
    #--------- --------- --------- --------- --------- --------- --------- --------- --------- ---------
    # 5개년 주요 투자지표 정리
    if len(df_5yr)>0:
        df = df_5yr
        df = df.melt(id_vars=['종목코드', '종목명'], var_name='항목', value_name='값').reset_index(drop=True)
        df.columns.name = None
        df = clean_numeric_value(df.dropna())
        df.to_pickle(folder_itooza_backup + f'1_장기투자지표_5개년_{suffix}.pkl') # 파일 백업
        del df_5yr
    #--------- --------- --------- --------- --------- --------- --------- --------- --------- ---------
    # 최근 지표 요약본 정리
    if len(df_short)>0:
        df = df_short
        df = df.melt(id_vars=['종목코드', '종목명'], var_name='항목', value_name='값').reset_index(drop=True)
        df.columns.name = None
        df = clean_numeric_value(df.dropna())
        df.to_pickle(folder_itooza_backup + f'2_최근지표요약_{suffix}.pkl') # 파일 백업
        del df, df_short
    print('(전체 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
    # 아이투자 지표정리------------- ------------- ------------- ------------- ------------- ------------- --------------

@helper.timer
def clean_itooza_longterm_indexes(): # 장기지표 평균치와 최근 지표 테이블 정리
    start_time = helper.now_time()
    print('아이투자 시계열 지표 계산해서 합치기 작업 시작 ' + helper.now_time().strftime('%Y-%m-%d %H:%M:%S'))
    # 파일 백업중 가장 5개년 파일 불러오기
    files = glob(folder_itooza_backup + '1_장기투자지표_5개년_*.pkl')
    files.reverse()
    df_all = pd.concat([pd.read_pickle(data) for data in files])
    df_all = df_all.drop_duplicates(subset=['종목코드','종목명','항목']).reset_index(drop=True)
    df_all = df_all.pivot_table(index='종목코드', columns='항목', values='값').reset_index()
    df_all.columns.name=None

    #----------- ----------- ----------- ----------- ----------- ----------- ----------- ----------- ----------- -----------
    df_series = pd.read_pickle(folder_itooza_backup + "0_아이투자_시계열_원본취합본.pkl")
    # 1. 연간의 경우 분기값이 포함되어 있는 경우가 있기 때문에 삭제해 주어야함
    # 삭제 방법은 가장 마지막에 위치한 날짜의 월 부분이 결산월과 일치하면 keep, 아니면 제외
    df_not_year = df_series[(df_series['기준']!='연간')] # 연간이 아닌 것만 들어 있는 df
    df_year = df_series[df_series['기준']=='연간'] # 연간만 들어 있는 df
    df_year = df_year.sort_values(by=['날짜'], ascending=False)

    # 연간만 들어 있는 df에 대해서 아래 작업하고 나서 다시 연간이 아닌 것과 합치기
    df_year['key'] = df_year['날짜'] + df_year['기준'] + df_year['종목코드'] # 날짜,기준,종목코드 key값 생성
    df_year_temp = df_year.groupby(['종목코드', '기준'], as_index=False).head(2) # 가장 최근값 2개만 선택
    co_info_fiscal_year = conn_db.from_("DB_기업정보", '취합본')[['종목코드','결산월']] # master에서 결산월 가져오기

    # 회사별로 가장 위에 있는 2개 날짜의 월부분이 결산월과 일치 하지 않으면 제외대상
    df_year_temp = df_year_temp.merge(co_info_fiscal_year, on='종목코드', how='inner')
    filt = df_year_temp['날짜'].str.split('년', expand=True)[1] != df_year_temp['결산월']
    exclude_list = df_year_temp[filt]['key'].tolist() # 제외대상의 key 값 list
    filt = df_year['key'].isin(exclude_list) # 년도만 있는 df의 key값이 제외대상에 있나없나 확인
    df_year = df_year.loc[~filt, : ] # 제외대상에 없는 것들만 선택
    df_year.drop(columns='key', inplace=True) # 작업하기 위해 임시로 만들었던 컬럼 삭제
    df_series = pd.concat([df_year, df_not_year]).reset_index(drop=True) # 다시 전체 df만들기

    #----------- ----------- ----------- ----------- -----------
    # 날짜에 '월'만 있는 행이 생겨서 삭제
    filt = df_series['날짜'] != '월'
    df_series = df_series.loc[filt].copy()

    df_temp = df_series.copy()
    matcher = ['ROE', '률']
    all_cols = df_temp.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df_temp[col] = df_temp[col]/100
    df_temp.rename(columns={'시가': '시가배당률', '주당': '주당배당금',
                            '주당순이익(EPS,개별)': 'EPS (개별)',
                            '주당순이익(EPS,연결지배)': 'EPS (연결지배)',
                            '주당순자산(지분법)' : 'BPS (지분법)'}, inplace=True)
    df_temp = helper.make_keycode(df_temp)  # KEY컬럼 만들기
    conn_db.to_(df_temp,'from_아이투자','아이투자_시계열_final')
    del exclude_list, filt, df_year_temp, co_info_fiscal_year, df_year, df_not_year, df_temp
    print('아이투자 시계열용 저장완료')

    #----------- ----------- ----------- ----------- -----------
    '''
    시계열 파일에서 ['10년치평균값', '10분기연환산평균값', '최근연환산값', '최근연간값', '최근분기값']를 계산해야함
    그리고 5년치와 최근4분기치와 합쳐서 모든 지표가 들어가 있는 테이블을 만든다
    '''
    # 2. 10년치 평균값과 10분기 연환산 평균값 구하기
    use_cols = ['종목코드', '기준','날짜', 'PBR', 'PER', 'ROE', '순이익률',
                '영업이익률', '주당순이익(EPS,개별)', '주당순이익(EPS,연결지배)', '주당순자산(지분법)']

    df = df_series[use_cols] # 10년평균 구할수 있는 값만 선택
    # 연환산 10Q와 연간10Y 평균값 가져오기 (최대/최소값은 제외)
    term_types = ['연환산','연간']
    term_type_name = {'연환산':'_10Q', '연간':'_10Y'}
    result_df = pd.DataFrame()
    for term_type in term_types:
        # ['연환산','연간'] 필터링
        temp_all = df.loc[df['기준']== term_type].sort_values(by='날짜', ascending=False).copy()
        # 4번부터가 투자지표 컬럼. 지표별로 돌면서 평균값구하기
        for col in temp_all.columns.tolist()[3:]:
            # 종목별로 돌면서 계산
            for co in temp_all['종목코드'].unique().tolist():
                # 단일종목, 단일지표 전체값 선택
                temp = temp_all.loc[temp_all['종목코드']==co, col].copy()
                temp_list = temp.tolist()
                new_col_name = col + term_type_name[term_type]
                try:
                    temp_list.remove(temp.max()) # 최대값제거
                    temp_list.remove(temp.min()) # 최소값제거
                    avg_10 = pd.Series(temp_list).mean() # 최대/최소 제거한 값의 평균
                     # 계산된 값의 명칭
                    result_df = result_df.append(pd.DataFrame([co, new_col_name, avg_10]).T)
                except: # 계산이 안될경우 공란으로 값 넣기
                    result_df = result_df.append(pd.DataFrame([co, new_col_name, None]).T)
    result_df.columns = ['종목코드','항목','값']
    result_df['값'] = pd.to_numeric(result_df['값'])
    result_df = result_df.pivot_table(index='종목코드', columns='항목', values = '값').reset_index()
    result_df.columns.name=None
    # 10년치 평균값과 10분기 연환산 평균값 구한건 본래 있던 거에 합치기
    df_all = df_all.merge(result_df, on='종목코드', how='outer')
    del temp_all, temp, result_df
    print('아이투자 10년치 평균값과 10분기 연환산 평균값 구하는 작업완료')

    #----------- ----------- ----------- ----------- -----------
    # 3. 연환산, 연간, 분기의 가장 최근에 있는 값만 가져오기
    # 필요한 컬럼만 필터링하고 날짜순으로 정렬
    use_cols = ['날짜', '기준','종목코드', 'PBR','PER','ROE', '순이익률', '영업이익률',
                '주당순이익(EPS,개별)', '주당순이익(EPS,연결지배)', '주당순자산(지분법)']
    df = df_series.loc[:, use_cols].sort_values(by='날짜', ascending=False)

    # 기준별로 가장 최근 날짜만 선택
    df = df.groupby(['종목코드', '기준'], as_index=False).head(1)
    df = df.melt(id_vars=['날짜','기준','종목코드'], value_name='값', var_name=['항목']) # tidy로 수정
    df = df.sort_values(by='날짜', ascending=False) # 날짜 순으로 정렬

    # 항목이름 뒤에 날짜기준 추가하기
    term_type_name = {'연환산':'_최근연환산', '연간':'_최근Y', '분기': '_최근Q'}
    df['항목'] = df['항목'] + [term_type_name[x] for x in df['기준']]
    # 필요없는 컬럼 삭제
    df.drop(columns=['날짜', '기준'], inplace=True)
    df = df.pivot_table(index='종목코드', columns= '항목', values='값').reset_index()
    df.columns.name= None
    # 연환산, 연간, 분기의 가장 최근에 있는 값과 본래 있던 거에 합치기
    df_all = df_all.merge(df, on='종목코드', how='outer').reset_index(drop=True)
    del df, df_series
    print('아이투자 연환산 / 연간 / 분기에서 가장 최근값만 가져오기 작업완료')

    #----------- ----------- ----------- ----------- -----------
    matcher = ['ROE', '률']
    all_cols = df_all.columns.tolist()
    prcnt_cols = [col for col in all_cols if any(prcnt in col for prcnt in matcher)]
    for col in prcnt_cols:
        df_all[col] = df_all[col]/100
    col_name_change = {'주당순이익(EPS,개별)_최근Q': 'EPS_최근Q (개별)',
                        '주당순이익(EPS,개별)_최근Y': 'EPS_최근Y (개별)',
                        '주당순이익(EPS,개별)_최근연환산': 'EPS_최근연환산 (개별)',
                        '주당순이익(EPS,개별)_10Q': 'EPS_10Q (개별)',
                        '주당순이익(EPS,개별)_10Y': 'EPS_10Y (개별)',
                        '주당순이익(EPS,연결지배)_최근Q' : 'EPS_최근Q (연결지배)',
                        '주당순이익(EPS,연결지배)_최근Y': 'EPS_최근Y (연결지배)',
                        '주당순이익(EPS,연결지배)_최근연환산' : 'EPS_최근연환산 (연결지배)',
                        '주당순이익(EPS,연결지배)_10Q': 'EPS_10Q (연결지배)',
                        '주당순이익(EPS,연결지배)_10Y': 'EPS_10Y (연결지배)',
                        '주당순자산(지분법)_최근Q' : 'BPS_최근Q (지분법)',
                        '주당순자산(지분법)_최근Y' : 'BPS_최근Y (지분법)',
                        '주당순자산(지분법)_최근연환산' : 'BPS_최근연환산 (지분법)',
                        '주당순자산(지분법)_10Q' : 'BPS_10Q (지분법)',
                        '주당순자산(지분법)_10Y': 'BPS_10Y (지분법)',
                        '5년PER': 'PER_5Y',
                        '5년PBR': 'PBR_5Y',
                        '5년ROE': 'ROE_5Y',
                        '5년EPS성장률': 'EPS_5Y%',
                        '5년BPS성장률': 'BPS_5Y%'}
    for col in all_cols:
        if col in col_name_change:
            df_all.rename(columns={col: col_name_change[col]}, inplace=True)

    #----------- ----------- ----------- ----------- -----------
    # DB_기업정보 FS_update_list에 있는 코드만 필터링
    global code_list
    df_all = df_all[df_all['종목코드'].isin(code_list)].copy()
    df_all = helper.make_keycode(df_all).drop(columns=['종목명', '종목코드'])

    df_all.to_pickle(folder_itooza + '장기투자지표_취합본.pkl')
    merge_df_all_numbers() # 전체 취합본 업데이트

#아이투자 기업정보------------- ------------- -------------  ------------- ------------- ------------- --------------
def get_itooza_company_description(code):
    global company_description  # 전체 내용있는것
    # global raw_material_1_df   # 원재료_가로형
    # global raw_material_2_df  # 원재료_세로형
    # global product_1_df  # 제품_가로형
    # global product_2_df  # 제품_세로형
    url = f"http://search.itooza.com/search.htm?seName={code}&jl=k&search_ck=&sm=&sd=&ed=&ds_de=&page=&cpv="
    r = requests.get(url, headers={'User-Agent': user_agent})
    time.sleep(1)
    # r.encoding = r.apparent_encoding
    r.encoding ='euc-kr'
    #----------- ----------- ----------- ----------- ----------- -----------
    # 전체 df만들기
    dom = BeautifulSoup(r.text, "html.parser")
    title = dom.select('#content > div.box120903 > div.ainfo_com > div > table > tr > th')
    title = [x.text.replace('\r', '').replace('\t', '').replace('\n', '-') for x in title]
    content = dom.select('#content > div.box120903 > div.ainfo_com > div > table > tr > td')
    content = [x.text.strip() for x in content]
    df = pd.DataFrame([title, content]).T
    df[['구분', '기준날짜']] = df[0].str.split('-', expand=True)
    df['기준날짜'] = df['기준날짜'].str.replace('.', '/')
    df = df.drop(columns=0).rename(columns={1: '내용'})
    df['종목코드'] = code
    df = df[['종목코드', '구분', '내용', '기준날짜']]
    company_description = company_description.append(df)
    # #제품 df ----------- ----------- ----------- ----------- ----------- -----------
    # products = []
    # for x in content[2].split(':'):
    #     products.append(x.split('-')[0])
    # products = ' '.join(products).replace(' 등 ', ', ').replace('* 괄호 안은 순매출액 비중', '').strip()
    # products = products.replace('* 괄호 안은 매출 비중','').strip()
    # products = products.replace('  ', ' ')
    # # 제품 가로형
    # product_1 = pd.DataFrame([products]).rename(columns={0: '제품'})
    # product_1['종목코드'] = code
    # product_1_df = product_1_df.append(product_1)
    # # 제품 세로형
    # product_2 = pd.DataFrame(products[:-1].split(', ')).rename(columns={0: '제품'})
    # product_2['종목코드'] = code
    # product_2_df = product_2_df.append(product_2)
    # #원재료 df ----------- ----------- ----------- ----------- ----------- -----------
    # raw_materials = []
    # for x in content[3].split('-'):
    #     try:
    #         raw_materials.append(x.split('(')[0].split(' ', 1)[1].strip())
    #     except:
    #         pass
    # # 원재료 가로형
    # raw_material_1 = pd.DataFrame([', '.join(raw_materials)]).rename(columns={0: '원재료'})
    # raw_material_1['종목코드'] = code
    # raw_material_1_df = raw_material_1_df.append(raw_material_1)
    # # 원재료 세로형
    # raw_material_2 = pd.DataFrame(raw_materials).rename(columns={0: '원재료'})
    # raw_material_2['종목코드'] = code
    # raw_material_2_df = raw_material_2_df.append(raw_material_2)
    #------------------------------------------------------------------------------------------------------------------------

@helper.timer
def update_itooza_company_description(param='all'):
    start_time = helper.now_time()
    global company_description  # 전체 내용있는것
    # global raw_material_1_df   # 원재료_가로형
    # global raw_material_2_df  # 원재료_세로형
    # global product_1_df  # 제품_가로형
    # global product_2_df  # 제품_세로형 
    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     executor.map(get_itooza_company_description, code_list)
    if param !='all':
        new = conn_db.from_("DB_기업정보", 'FS_update_list')['종목코드']
        old = conn_db.from_("DB_기업정보", 'from_아이투자_기업정보')['종목코드']
        new_code_list = list(set(new) - set(old))
        if len(new_code_list)>0:
            [get_itooza_company_description(code) for code in new_code_list]
        else:
            print('업데이트할 내역 없음')
        del new, old, new_code_list
    else:
        [get_itooza_company_description(code) for code in code_list]
        
    # 전체 내용있는것------- ------- ------- ------- ------- ------- ------- ------- 
    if len(company_description)>=0:
        print('가져오기 완료. 전처리 시작')
        try:
            company_description = helper.make_keycode(company_description.reset_index(drop=True))
        except: # 새로 가져온 것이 없을 경우 그냥 pass
            pass

        # 가장 최근 파일이 위로 가도록 순서 정렬해서 취합하고 과거 df랑 중복 되는거 삭제
        company_description['내용'] = company_description['내용'].apply(lambda x : x.replace('▷ ','\n▷ ' ).strip() if '▷ 'in x else x.replace('▷','\n▷ ' ).strip() if '▷' in x else x )
        old_df = conn_db.from_('DB_기업정보', 'from_아이투자_기업정보')
        cols = ['구분', 'KEY']
        company_description = drop_duplicate_rows(company_description, old_df, cols)
        conn_db.to_(company_description, 'DB_기업정보', 'from_아이투자_기업정보')
        col_name_dict = {'주요제품':'제품명', 
                        '원재료':'원재료'}

        for col_names in col_name_dict.keys():
            col_name = col_name_dict[col_names]
            df_temp = company_description[company_description['구분']==col_names].reset_index(drop=True)
            df_result = pd.DataFrame()
            #----아래부터 가로형 만들기 작업
            for x in df_temp['KEY'].unique().tolist():
                temp = df_temp.loc[df_temp['KEY']==x].copy()
                temp = temp['내용'].str.split('▷', expand=True).T
                temp.rename(columns={temp.columns.tolist()[0]:col_name}, inplace=True)
                temp['KEY'] = x
                df_result = df_result.append(temp)
            filt = df_result[col_name].apply(len)<2
            df_result = df_result.loc[~filt].copy().reset_index(drop=True)
            df_result[col_name] = df_result[col_name].str.strip()

            #----아래부터 세로형 만들기 작업
            filt = df_result[col_name].str.contains(':')
            df_result1 = df_result[filt] # ':' 있는거
            df_result2 = df_result[~filt] # ':' 없는거

            df_result_1_1 = pd.DataFrame()
            for x in df_result1['KEY'].unique().tolist():
                temp = df_result1.loc[df_result1['KEY']==x].copy()
                temp = temp[col_name].str.split(':',1, expand=True)
                temp.rename(columns={temp.columns.tolist()[0]:col_name}, inplace=True)
                temp['KEY'] = x
                df_result_1_1 = df_result_1_1.append(temp)
            filt = df_result_1_1[col_name].apply(len)<2
            df_result_1_1 = df_result_1_1.loc[~filt].copy()
            df_result_1_1.rename(columns={1:'내용'}, inplace=True)

            df_result_2_1 = pd.DataFrame()
            for x in df_result2['KEY'].unique().tolist():
                temp = df_result2.loc[df_result2['KEY']==x].copy()
                temp = temp[col_name].str.split('(', 1, expand=True)
                temp.rename(columns={temp.columns.tolist()[0]:col_name}, inplace=True)
                temp['KEY'] = x
                df_result_2_1 = df_result_2_1.append(temp)
            filt = df_result_2_1[col_name].apply(len)<2
            df_result_2_1 = df_result_2_1.loc[~filt].copy()
            df_result_2_1.rename(columns={1:'내용'}, inplace=True)

            df_result_long = pd.concat([df_result_1_1, df_result_2_1]).reset_index(drop=True)
            df_result_long[col_name] = df_result_long[col_name].str.strip()
            df_result_long['내용'] = df_result_long['내용'].str.strip()

            df_temp = df_temp[['KEY','종목코드','기준날짜','종목명']].copy()

            df_result = df_temp.merge(df_result, on='KEY', how='right')
            conn_db.to_(df_result, 'DB_기업정보', f'{col_name}_가로형')

            df_result_long = df_temp.merge(df_result_long, on='KEY', how='right')
            conn_db.to_(df_result_long, 'DB_기업정보', f'{col_name}_세로형')
        del company_description, df_result1, df_result2, temp
        del df_temp, df_result, df_result_long, df_result_2_1, df_result_1_1, filt
        # # 원재료_가로형------ ------- ------- ------- ------- ------- ------- -------
        # raw_material_1_df = helper.make_keycode(raw_material_1_df.reset_index(drop=True))
        # conn_db.to_(raw_material_1_df, 'DB_기업정보', '원재료_가로형')

        # # 원재료_세로형------ ------- ------- ------- ------- ------- ------- -------
        # raw_material_2_df = helper.make_keycode(raw_material_2_df.reset_index(drop=True))
        # conn_db.to_(raw_material_2_df, 'DB_기업정보', '원재료_세로형')

        # # 제품_가로형------ ------- ------- ------- ------- ------- ------- -------
        # product_1_df = helper.make_keycode(product_1_df.reset_index(drop=True))
        # conn_db.to_(product_1_df, 'DB_기업정보', '제품_가로형')

        # # 제품_세로형------ ------- ------- ------- ------- ------- ------- -------
        # product_2_df = helper.make_keycode(product_2_df.reset_index(drop=True))
        # conn_db.to_(product_2_df, 'DB_기업정보', '제품_세로형')
        #------- ------- -------
        # , raw_material_1_df, raw_material_2_df, product_1_df, product_2_df
        import co_info as co
        co.get_all_co_info() # 전체 취합본 업데이트 해놓기 
    else:
        print('업데이트할 내역 없음')
#--------------------------------------------------------------------------------------------------------------------
def update_and_clean_all():
    total_start_time = helper.now_time()
    #----------------- -------------------------
    print('네이버 증권 시작')
    start_time = helper.now_time()
    update_naver_fs()
    print('(네이버 증권 가져오기 및 전처리 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
    #----------------- -------------------------
    print('아이투자 투자지표 시작')
    start_time = helper.now_time()
    update_itooza_fsratio()
    print('(아이투자 투자지표 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
    start_time = helper.now_time()
    print('(아이투자 5개년 지표 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
    clean_itooza_longterm_indexes()
    print('아이투자 전처리 완료')
    #----------------- -------------------------
    print('fnguide 재무제표 시작')
    start_time = helper.now_time()
    update_fnguide_fs()
    print('(fnguide 가져오기 및 전처리 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
    #----------------- -------------------------
    print('fnguide 투자지표 시작')
    start_time = helper.now_time()
    update_fnguide_invest_ratio()
    print('(fnguide 투자지표 가져오기 및 전처리 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
    #----------------- -------------------------
    print('fnguide 재무비율 시작')
    start_time = helper.now_time()
    update_fnguide_fsratio()
    clean_fsratio_from_fnguide()
    print('(fnguide 재무비율 가져오기 및 전처리 완료. 소요시간: ' + str(helper.now_time() - start_time)+")")
    #----------------- -------------------------
    print('(총 소요시간: ' + str(helper.now_time() - total_start_time)+")")
#--------------------------------------------------------------------------------------------------------------------
