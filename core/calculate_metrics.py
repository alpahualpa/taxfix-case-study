# Author: Nikita Silin
# Task: Taxfix Case Study
# Purpose: Report generation

import sqlite3
import pandas as pd


def metric_calculation(metric_query):

    con = sqlite3.connect("persons.db")

    cur = con.cursor()

    res3 = cur.execute(metric_query)

    res3 = res3.fetchall()

    return res3


def metric_1(accuracy=2):
    metric_query = '''
        with c1 as (
            select count(*) as cnt
            from persons 
            where lower([email]) like '%gmail.%'
            and [address.country] = 'Germany'
        ), c2 as (
            select count(*) as cnt from persons
        )
        select cast(round(100.0 * c1.cnt / c2.cnt, ''' + str(accuracy) + ''') as text) || '%' as answer
        from c1, c2
    '''

    return_metric = metric_calculation(metric_query)[0][0]

    return return_metric


def metric_2(rank=3):
    metric_query = '''
        with c1 as (
            select
                [address.country] as country,
                count(*) as count
            from persons
            where lower([email]) like '%gmail.%'
            group by country
        ), c2 as (
            select
                country,
                dense_rank() over (
                    order by count desc
                ) as d_rank,
                count
            from c1
        )
        select
            country,
            d_rank as rank,
            count as persons_amount
        from c2
        where d_rank <= ''' + str(rank) + '''
    '''

    return_metric = pd.DataFrame(metric_calculation(metric_query))

    return_metric.columns = [['country', 'rank', 'persons_amount']]

    return return_metric


def metric_3():
    metric_query = '''
        with c1 as (
            select
                *,
                cast(substr([age], 1, INSTR([age], '-')-1) as integer) as age_f
            from persons
            where lower([email]) like '%gmail.%'
        )
        select
            count(*)
        from c1
        where age_f >= 60
    '''

    return_metric = str(metric_calculation(metric_query)[0][0])

    return return_metric


def generate_report():

    q1 = '''
Question 1.
Which percentage of users live in Germany and use Gmail as an email provider?
'''

    a1 = metric_1()

    q2 = '''
Question 2.
Which are the top three countries in our database that use Gmail as an email provider?
'''

    a2 = metric_2()

    q3 = '''
Question 3.
How many people over 60 years use Gmail as an email provider?
    '''

    a3 = metric_3()

    print('-------------------------------------------')
    print('Report\n')

    print(q1)
    print('Answer 1.')
    print(a1)
    print(q2)
    print('Answer 2.')
    print(a2)
    print(q3)
    print('Answer 3.')
    print(a3)


if __name__ == '__main__':
    generate_report()

