#!/usr/bin/env python3

import sqlite3
import os
import datetime
import argparse
import subprocess

class position:
    def __init__(self, line):
        line = line.strip()
        self.account, self.value, self.currency = line.split()

    def get_account(self):
        return(self.account)

    def get_value(self):
        return(self.value)

class budget:
    def __init__(self, db, beanfile, month=None, year=None):
        self.beanfile = beanfile
        today = datetime.date.today()
        if month == None:
            self.month = today.month
        else:
            self.month = month
        if year == None:
            self.year = today.year
        else:
            self.year = year
        self.connect(db)
        self.bq = "bean-query"
        self.tempfile = 'beanvelope.tmp'
        self.get_budget_id()
        self.return_codes = {
                1: "income_inserted",
                2: "income_update",
                3: "income_change_fail",
                4: "get_account_id_fail"
                }


    def connect(self,db):
        # Open a connection to the beanvelope (sqlite) database 
        self.dbobject = sqlite3.connect(db)
        self.curs = self.dbobject.cursor()

    def close(self):
        self.dbobject.close()

    def write_temp(self, contents):
        with open(self.tempfile, 'w') as target:
            target.write(contents)

    def read_temp(self):
        with open(self.tempfile, 'r') as target:
            a = target.readlines()
        return(a[2:])

    def run_beancount(self, query):
        output = subprocess.check_output([self.bq, self.beanfile, query])
        results = output.decode('utf-8')
        self.write_temp(results)

    
    def get_bean_accounts(self):
        query = "balances where account ~ 'Expenses' or (account ~ 'Liabilities' and not 'Expenses:Interest' in other_accounts) and month = {} and year = {}".format(self.month, self.year)
        self.run_beancount(query)

    def get_bean_income(self):
        query = "select 'Income',sum(position) where month = {} and year = {} and account ~ 'Income' and not 'Exclude' in tags group by 'Income'".format(self.month, self.year)
        self.run_beancount(query)

    #TODO Use this for all insert, updates or deletes
    def write_sql(self, sql, params, get_id=False):
        try:
            if len(params) > 1:
                go = self.curs.executemany(sql, params)
            else:
                go = self.curs.execute(sql, params)
        except sqlite3.IntegrityError:
            return("constraint_violation")
        except:
            return("sql_failure")
        else:
            self.dbobject.commit()
            if get_id == True:
                return(self.curs.lastrowid)
            else:
                return(0)

    def read_sql(self, sql, params, single=False):
        try:
            go = self.curs.execute(sql, params)
        except:
            return("sql_failure")
        else:
            if single == False:
                result = go.fetch()
            else:
                result = go.fetchone()
            return(result)




    def insert_accounts(self):
        accounts = self.read_temp()
        for row in accounts:
            entry = position(row)
            sql = '''insert into accounts (account_name) values (?)'''
            try:
                go = self.curs.execute(sql, [str(entry.get_account())])
            except sqlite3.IntegrityError:
                pass
            else:
                self.dbobject.commit()
                self.account_id = self.curs.lastrowid
                print("Added:",entry.get_account(), "ID:",self.account_id)

    def load_income(self):
        income = self.read_temp()
        entry = position(income[0])
        sql = '''insert into income values (?, ?)'''
        try:
            go = self.curs.execute(sql, [str(self.budget_id), str(entry.get_value())])
        except sqlite3.IntegrityError:
            sql = '''update income set income = ? where budget_id = ?'''
            try:
                go = self.curs.execute(sql, [str(entry.get_value()),str(self.budget_id)])
            except:
                print("Failed")
            else:
                self.dbobject.commit()
                return(1)
        else:
            self.dbobject.commit()
            print("Inserted income")

    def load_accounts(self):
        accounts = self.read_temp()
        load_list = []
        for row in accounts:
            entry = position(row)
            pair = (entry.get_value(), entry.get_account())
            load_list.append(pair)
        print(load_list)
        sql = '''update budget_base 
                 set spending = ? 
                 where account_id = (select account_id from accounts where account_name = ?)
                 '''
        go = self.curs.executemany(sql, load_list)
        self.dbobject.commit()
        print("Updated income")



    def open_budget(self):
        '''Create a new entry in the budgets table'''
        sql = '''insert into budgets (year,month) values (?, ?)'''
        try:
            go = self.curs.execute(sql, [str(self.year), str(self.month)])
        except sqlite3.IntegrityError:
            print("This already exists")
        else:
            self.dbobject.commit()
            self.budget_id = self.curs.lastrowid
            print(self.budget_id)

        
    def get_budget_id(self):
        sql = '''select budget_id from budgets where year = ? and month = ?'''
        go = self.curs.execute(sql, [str(self.year), str(self.month)])
        result = go.fetchone()
        self.budget_id = result[0]

    def get_account_id(self,name):
        sql = '''select account_id from accounts where account_name = ?'''
        #go = self.curs.execute(sql, [name])
        #result = go.fetchone()
        #try:
        #    self.account_id = result[0]
        #except:
        #    return(4)
        #else:
        #    go = self.curs.execute(sql, [name])
        #    self.dbobject.commit()
        result = self.read_sql(sql, [name])
        return(result)

    def set_base_envelope(self,acct_id,value):
        sql = '''update budget_base
                set base_value = ?
                where budget_id = ?
                and account_id = ?'''

if __name__ == "__main__":
    a = budget("test.db", "test3.bean",12)
    a.open_budget()
    a.get_bean_accounts()
    a.insert_accounts()
    print("Budget ID: ", a.budget_id)
    a.get_bean_income()
    a.load_income()
    a.get_bean_accounts()
    a.load_accounts()
    a.close()


