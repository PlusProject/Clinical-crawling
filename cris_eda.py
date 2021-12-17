# %%
from bs4 import BeautifulSoup
from selenium import webdriver as wd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import requests
import pymysql
import time
import sys
import re
from webdriver_manager.chrome import ChromeDriverManager
import sqlalchemy
# 데이터베이스 연결
db_connection_str = 'mysql+pymysql://admin:tjdrbsrhkseo123@database-skku.c6dzc5dnqf69.ap-northeast-2.rds.amazonaws.com/medii'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()


# url 크롤링
main_url = 'https://cris.nih.go.kr/cris/search/listDetail.do'
driver = wd.Chrome(ChromeDriverManager().install())
page_list = []

driver.get(main_url)
driver.find_element_by_css_selector('button.btnSearch').click()
driver.implicitly_wait(1)

paging_list = driver.find_element_by_css_selector("#paging a:nth-child(12)")
temp_link = paging_list.get_attribute("href")
num_index = temp_link.find('#')
page_len = int(temp_link[num_index+1:])

for page in range(1, page_len+1):
    try:
        driver.find_element(By.XPATH, "//a[@href='#%s']"%page).send_keys(Keys.ENTER);
        print("%s 페이지 이동" % page)
        time.sleep(1.5)

        link_list = driver.find_elements_by_css_selector("#example tbody tr td:nth-child(5) a")
        for link in link_list:
            page_num = link.get_attribute("href")
            num_index = page_num.find('(')
            page_num = page_num[num_index+1:-1]
            page_list.append(page_num)
        time.sleep(1)
            
    except Exception as e1:
        print( '페이지 검색 오류', e1 )

driver.close()
driver.quit()




# %%
#page_list = ['15798', '12854', '12186', '18912', '12677', '16661', '11949', '19300']

# %%
# cris 크롤링

#css 선택자 딕셔너리
static_selector = {'reg_num' : '#step1 > div > table > tbody > tr:nth-of-type(2) > td',
                   'brief_title_kor' : '#step1 > div > table > tbody > tr:nth-of-type(4) > td',
                   'brief_title_eng' : '#step1 > div > table > tbody > tr:nth-of-type(5) > td',
                   'main_title_kor':"#step1 > div > table > tbody > tr:nth-of-type(6) > td",
                   'main_title_eng':'#step1 > div > table > tbody > tr:nth-of-type(7) > td',
                   
                   'recruitment_state' : '#step4 > div > table > tbody > tr:nth-of-type(3) > td',
                   'start_date' : '#step4 > div > table > tbody > tr:nth-of-type(4) > td',
                   #'자료수집종료일' : '#step4 > div > table > tbody > tr:nth-of-type(6) > td',
                   'end_date' : '#step4 > div > table > tbody > tr:nth-of-type(7) > td',

                   #'연구책임기관_국문' : '#step6 > div > table > tbody > tr:nth-of-type(3) > td',
                   #'연구책임기관_영문' : '#step6 > div > table > tbody > tr:nth-of-type(4) > td',

                   'study_summary_kor':'#step7 > div > table > tbody > tr:nth-of-type(2) > td > pre',
                   'study_summary_eng':'#step7 > div > table > tbody > tr:nth-of-type(3) > td > pre',

                   
                   'chief_name_kor':'#step3 > div > table > tbody > tr:nth-of-type(3) > td',
                   'chief_name_eng':'#step3 > div > table > tbody > tr:nth-of-type(4) > td',
                   'chief_job_kor' : '#step3 > div > table > tbody > tr:nth-of-type(5) > td',
                   'chief_job_eng' : '#step3 > div > table > tbody > tr:nth-of-type(6) > td',
                   'chief_belong_kor' : '#step3 > div > table > tbody > tr:nth-of-type(8) > td',
                   'chief_belong_eng' : '#step3 > div > table > tbody > tr:nth-of-type(9) > td',
                   'chief_position_kor' : '#step3 > div > table > tbody > tr:nth-of-type(10) > td',
                   'chief_position_eng' : '#step3 > div > table > tbody > tr:nth-of-type(11) > td',

                   'charge_name_kor':'#step3 > div > table > tbody > tr:nth-of-type(13) > td',
                   'charge_name_eng':'#step3 > div > table > tbody > tr:nth-of-type(14) > td',
                   'charge_job_kor' : '#step3 > div > table > tbody > tr:nth-of-type(15) > td',
                   'charge_job_eng' : '#step3 > div > table > tbody > tr:nth-of-type(16) > td',
                   'charge_belong_kor' : '#step3 > div > table > tbody > tr:nth-of-type(18) > td',
                   'charge_belong_eng' : '#step3 > div > table > tbody > tr:nth-of-type(19) > td',
                   'charge_position_kor' : '#step3 > div > table > tbody > tr:nth-of-type(20) > td',
                   'charge_position_eng' : '#step3 > div > table > tbody > tr:nth-of-type(21) > td',

                   'rm_name_kor':'#step3 > div > table > tbody > tr:nth-of-type(23) > td',
                   'rm_name_eng':'#step3 > div > table > tbody > tr:nth-of-type(24) > td',
                   'rm_job_kor' : '#step3 > div > table > tbody > tr:nth-of-type(25) > td',
                   'rm_job_eng' : '#step3 > div > table > tbody > tr:nth-of-type(26) > td',
                   'rm_belong_kor' : '#step3 > div > table > tbody > tr:nth-of-type(28) > td',
                   'rm_belong_eng' : '#step3 > div > table > tbody > tr:nth-of-type(29) > td',
                   'rm_position_kor' : '#step3 > div > table > tbody > tr:nth-of-type(30) > td',
                   'rm_position_eng' : '#step3 > div > table > tbody > tr:nth-of-type(31) > td',
                   }

dynamic_selector = {'target_disease':['#step9 > div > table > tbody > tr:nth-of-type(16) > td',
                              '#step9 > div > table > tbody > tr:nth-of-type(2) > td',
                              '#step9 > div > table > tbody > tr:nth-of-type(6) > td'],
                   'rare_disease':['#step9 > div > table > tbody > tr:nth-of-type(16) > td',
                                   '#step9 > div > table > tbody > tr:nth-of-type(5) > td',
                                  '#step9 > div > table > tbody > tr:nth-of-type(9) > td']}


# 텍스트 정리 함수
def get_text(s):
    text = html.select_one(s).text.strip().replace('\t','').replace('\r','').replace('\n',' ').replace('\xa0','')
    return text

# 동적 선택자 크롤링
def dynamic_crawling (name):
    result = [get_text(dynamic_selector[name][1]) if html.select_one(dynamic_selector[name][0]) == None else get_text(dynamic_selector[name][2])]
    return result


# 데이터프레임 컬럼명 입력
header_list = [s for s in static_selector] + [d for d in dynamic_selector] + ['cris_url']
cris_df = pd.DataFrame(columns=header_list)


# 크롤링
#count = 1
for i in page_list:
    url = "https://cris.nih.go.kr/cris/search/detailSearch.do?seq="+str(i)+"&write_step=&temp_seq=&status=5&seq_group="+str(i)+"&searchWord=&isOpen=&page=1&class_yn=&class_title=&class_title2=&research_title=&system_number=&research_kind=&research_step=&results_yn=&funding_agency=&sponsor_agency=&research_agency=&charge_name=&cp_contents=&primary_outcome=&secondary_outcome=&arm_desc_kr=&sub_date_s=&sub_date_e=&app_date_s=&app_date_e=&udt_date_s=&udt_date_e=&search_page=L&my_code=&research_nation=&share_yn=&hcb_approval_status=&funding_type=&target_rare_yn=&clinical_step=&target_in_sex=&target_age=&intervention_type=&search_yn=Y&search_lang=&searchGubun="
    raw = requests.get(url,headers={'USER-Agent':'Mozilla/5.0'})
    html = BeautifulSoup(raw.text, "html.parser")
    
    content_list = [get_text(s) for s in static_selector.values()] + dynamic_crawling('target_disease') + dynamic_crawling('rare_disease') + [url]
    
    cris_df.loc[len(cris_df)] = content_list

#     print(i,'upload',count)
#     count += 1

print('\nsuccess')

# %%
"""
# EDA
"""

# %%
# 타이틀과 연구진이 없는 이상값 제거
df = cris_df.dropna(subset=['main_title_kor','chief_name_kor'])

# 중복값 제거
df.drop_duplicates(inplace=True, ignore_index=True)




# disease 분리 -> disease_code, disease_kor, disease_eng

disease = np.array(df['target_disease'].tolist())

# 질병코드 추출
l = [None if i.find(')')==-1 else i[1:i.find(')')] for i in disease]

# target_disease에서 질병코드 제거
con = [v if v.find(')')==-1 else v.replace(l[i],'').replace('()','') for i,v in enumerate(disease)]

# 두 번째 질병코드 추출
l2 = [None if i.find(')')==-1 else i[i.find('(')+1:i.find(')')] for i in con]

# 질병코드 완성
l3 = [l[i] if l2[i] == None else l[i] + ' | '+l2[i] for i in range(0,len(l2))]

# 데이터프레임에 질병코드 추가
disease_code_df2 = pd.DataFrame(l3)
df = pd.concat([df,disease_code_df2],axis=1)
df.rename(columns={0:'disease_code'}, inplace=True)

# target_disease에서 두 번째 질병코드 제거
final = [v if v.find(')')==-1 else v.replace(l2[i],'').replace('()','') for i,v in enumerate(con)]
        
# disease (첫번째)영문만 추출
p = re.compile("[a-z A-Z,-_’]+")
con_eng = [p.match(i).group() if p.match(i) else i for i in final]


# disease에서 첫번째 영문 삭제
con_kor = [v.replace(con_eng[i],'').strip() for i,v in enumerate(final)]


# 국문 제거
con_kr_middle = [i.replace(re.match("[,ㄱ-힣 ]+",i).group(),'').strip() if re.match("[,ㄱ-힣 ]+",i) else i for i in con_kor]

# 영문만 남기기
eng2=[]
for i in con_kr_middle:
    p = re.match("[1()a-z A-Z_-’,-]+",i)
    if p:
        s=p.group()
    else:
        s='None'

    eng2.append(s)

# 문자열(,) 제거
eng3 = []
for i in eng2:
    if i[0]==',':
        s = i[1:]
    else:
        s = i
    eng3.append(s)

# None값 처리
eng4 = []
for i in eng3:
    if i =='None':
        s = None
    else:
        s =i
    eng4.append(s)

# 최종 disease_eng
con_eng_final = [con_eng[i].strip() if eng4[i] == None else con_eng[i].strip() + ' | '+ eng4[i] for i in range(0,len(con_eng))]

# 최종 disease_kor
con_kr_final = [v.replace(eng3[i],'| ').strip() for i,v in enumerate(con_kor)]
    
    
# 필요 없는 컬럼 삭제
df.drop("target_disease", axis=1, inplace=True)

# disease_kor 추가
disease_kor = pd.DataFrame(con_kr_final)
df = pd.concat([df,disease_kor],axis=1)
df.rename(columns={0:'disease_kor'}, inplace=True)


# disease_eng 추가
disease_eng = pd.DataFrame(con_eng_final)
df = pd.concat([df,disease_eng],axis=1)
df.rename(columns={0:'disease_eng'}, inplace=True)

# %%
# 희귀질환 flag
df.loc[df['rare_disease'] == '아니오(No)','rare_disease'] = 0
df.loc[df['rare_disease'] == '예(Yes)','rare_disease'] = 1

# cid 추가
df = df.rename_axis('cid').reset_index()

# %%
# 병원코드 추가



# %%
# 소속기관 영문이름 통합


# %%
"""
# 데이터베이스 저장
"""

# %%
dtypesql = {'cid':sqlalchemy.types.INTEGER(),
            'reg_num':sqlalchemy.types.VARCHAR(255), 
            'brief_title_kor':sqlalchemy.types.VARCHAR(2000), 
            'brief_title_eng':sqlalchemy.types.VARCHAR(2000), 
            'main_title_kor':sqlalchemy.types.VARCHAR(2000), 
            'main_title_eng':sqlalchemy.types.VARCHAR(2000), 
            'recruitment_state':sqlalchemy.types.VARCHAR(255), 
            'start_date':sqlalchemy.types.VARCHAR(255), 
            'end_date':sqlalchemy.types.VARCHAR(255), 
            'study_summary_kor':sqlalchemy.types.TEXT, 
            'study_summary_eng':sqlalchemy.types.TEXT, 
            'chief_name_kor':sqlalchemy.types.VARCHAR(255), 
            'chief_name_eng':sqlalchemy.types.VARCHAR(255), 
            'chief_job_kor':sqlalchemy.types.VARCHAR(255), 
            'chief_job_eng':sqlalchemy.types.VARCHAR(255), 
            'chief_belong_kor':sqlalchemy.types.VARCHAR(255), 
            'chief_belong_eng':sqlalchemy.types.VARCHAR(255), 
            'chief_position_kor':sqlalchemy.types.VARCHAR(255), 
            'chief_position_eng':sqlalchemy.types.VARCHAR(255), 
            'charge_name_kor':sqlalchemy.types.VARCHAR(255), 
            'charge_name_eng':sqlalchemy.types.VARCHAR(255), 
            'charge_job_kor':sqlalchemy.types.VARCHAR(255), 
            'charge_job_eng':sqlalchemy.types.VARCHAR(255), 
            'charge_belong_kor':sqlalchemy.types.VARCHAR(255), 
            'charge_belong_eng':sqlalchemy.types.VARCHAR(255), 
            'charge_position_kor':sqlalchemy.types.VARCHAR(255), 
            'charge_position_eng':sqlalchemy.types.VARCHAR(255), 
            'rm_name_kor':sqlalchemy.types.VARCHAR(255), 
            'rm_name_eng':sqlalchemy.types.VARCHAR(255), 
            'rm_job_kor':sqlalchemy.types.VARCHAR(255), 
            'rm_job_eng':sqlalchemy.types.VARCHAR(255), 
            'rm_belong_kor':sqlalchemy.types.VARCHAR(255), 
            'rm_belong_eng':sqlalchemy.types.VARCHAR(255), 
            'rm_position_kor':sqlalchemy.types.VARCHAR(255), 
            'rm_position_eng':sqlalchemy.types.VARCHAR(255), 
            'rare_disease':sqlalchemy.types.INTEGER(), 
            'cris_url':sqlalchemy.types.VARCHAR(2000), 
            'disease_code':sqlalchemy.types.VARCHAR(255), 
            'disease_kr':sqlalchemy.types.VARCHAR(255), 
            'disease_eng':sqlalchemy.types.VARCHAR(255), 
            'belong_code':sqlalchemy.types.INTEGER(), 
}

df.to_sql(name='cris_dataset', con=db_connection, if_exists='replace', index=False,dtype=dtypesql)

# %%


# %%


# %%


# %%


# %%


# %%


# %%


# %%


# %%


# %%
