#!/usr/bin/env python3

import sqlite3
import os
import datetime
import argparse
import subprocess
import re
from tabulate import tabulate
from termcolor import colored
from configparser import ConfigParser

config_file = os.path.expandvars("$HOME/.config/beanvelope/beanvelope.conf")

def db_in(value):
    a=str(value)
    r1 = re.compile('-?[0-9]+\.[0-9]{2}')
    r2 = re.compile('-?[0-9]+\.[0-9]{1}')
    r3 = re.compile('-?[0-9]+')
    s1 = re.compile('\.')
    if r1.match(a):
        result = int(s1.sub('',a))
    elif r2.match(a):
        result = 10*int(s1.sub('',a))
    elif r3.match(a):
        result = 100*int(a)
    else:
        print("Conversion failure")
        print("Entered:",value)
        exit(20)
    return(result)


def db_out(value):
    v = str(value)
    if v[0] == "-":
        negative = True
        v=v[1:]
    else:
        negative = False
    if len(v) == 1:
        result = "0.0"+v
    elif len(v) == 2:
        result = "0."+v
    else:
        result = v[0:-2] + '.' + v[-2:]
    if negative:
        result = "-" + result
    return(result)

class position:
    def __init__(self, line):
        line = line.strip()
        self.account, self.value, self.currency = line.split()

    def get_account(self):
        return(self.account)

    def get_value(self):
        return(self.value)

class budget:
    def __init__(self, db, beanfile, tempfile, month=None, year=None,init=False):
        self.beanfile = beanfile
        today = datetime.date.today()
        if month == None:
            self.month = today.month
        else:
            self.month = int(month)
        if year == None:
            self.year = today.year
        else:
            self.year = int(year)
        if self.month == 12:
            self.next_month = 1
            self.next_year = self.year + 1
            self.last_month = 11
            self.last_year = self.year
        elif self.month == 1:
            self.next_month = 2
            self.next_year = self.year
            self.last_month = 12
            self.last_year = self.year - 1
        else:
            self.next_month = int(self.month) + 1
            self.next_year = self.year
            self.last_month = int(self.month) - 1
            self.last_year = self.year 
        self.connect(db)
        self.bq = "bean-query"
        self.tempfile = tempfile
        if init:
            self.open_budget()
        else:
            self.get_budget_id()
            self.check_budget_status()
            self.get_bean_income()
            self.load_income()
            self.get_income()
            self.get_bean_accounts()
            self.load_accounts()
            self.insert_accounts()
            self.update_missing()


    def connect(self,db):
        '''Open a connection to the beanvelope (sqlite) database'''
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
        '''Create a tempfile containing the current list of accounts from beancount'''
        #query = "balances from month = {} and year = {} where account ~ 'Expenses' or (account ~ 'Liabilities' and not 'Expenses:Interest' in other_accounts) order by account".format(self.month, self.year)
        query = "select account,sum(position) from month = {} and year = {} where account ~ 'Expenses' or (account ~ 'Liabilities' and not 'Expenses:Interest' in other_accounts) group by account order by account".format(self.month, self.year)
        self.run_beancount(query)

    def get_bean_income(self):
        '''Create a tempfile containing the available income from beancount'''
        query = "select 'Income',sum(position) from month = {} and year = {}  where account ~ 'Income' and not 'Exclude' in tags group by 'Income'".format(self.last_month, self.last_year)
        self.run_beancount(query)

    def write_sql(self, sql, params, get_id=False,single=True,debug=False):
        if debug:
            if single == True:
                go = self.curs.execute(sql, params)
            else:
                go = self.curs.executemany(sql, params)
        else:
            try:
                if single == True:
                    go = self.curs.execute(sql, params)
                else:
                    go = self.curs.executemany(sql, params)
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

    def read_sql(self, sql, params, single=False,debug=False):
        if debug == True:
            go = self.curs.execute(sql, params)
            if single == False:
                result = go.fetchall()
            else:
                result = go.fetchone()
            return(result)
        try:
            go = self.curs.execute(sql, params)
        except:
            return("sql_failure")
        else:
            if single == False:
                result = go.fetchall()
            else:
                result = go.fetchone()
            return(result)




    def insert_accounts(self):
        '''Insert new accounts into the database, as needed'''
        accounts = self.read_temp()
        for row in accounts:
            entry = position(row)
            sql = '''insert into accounts (account_name) values (?)'''
            results = self.write_sql(sql, [str(entry.get_account())], get_id=True)
            if results == "constraint_violation":
                pass
            elif results == "sql_failure":
                exit(1)
            else:
                sql = '''insert into corrections values (?,?,?,?)'''
                corr = self.write_sql(sql, [self.budget_id,results,'C',0])

    def update_missing(self):
        '''Update a budget to include new expense accounts'''
        accounts = self.read_temp()
        # Generate temp table of this month's accounts
        sql = '''create temporary table budget_temp as
                    select budget_id, account_id
                    from budget_base
                    where budget_id = ?'''
        b_tmp = self.write_sql(sql, [self.budget_id])

        # Look for account
        sql = '''create temporary table accounts_temp as 
                    select a.account_id, b.budget_id 
                    from accounts a left outer join budget_temp b 
                    on a.account_id = b.account_id
                    where a.closed = 0'''
        tmp_write = self.write_sql(sql, [])
        sql = '''select account_id from accounts_temp
                 where budget_id is null'''
        tmp_read = self.read_sql(sql, [], single=False)
        val_list = []
        for i in tmp_read:
            val_list.append((self.budget_id, i[0]))
        #print(val_list)
        sql = '''insert into budget_base values (?,?,0,0,0)'''
        results = self.write_sql(sql, val_list, single = False)
        sql = '''insert into corrections values (?,?,'C',0)'''
        results = self.write_sql(sql, val_list, single = False)

    def load_income(self):
        income = self.read_temp()
        entry = position(income[0])
        
        sql = '''insert into income values (?, ?)'''
        #results = self.write_sql(sql, [self.budget_id, str(-100*float(entry.get_value()))])
        results = self.write_sql(sql, [self.budget_id, -1*db_in(entry.get_value())])
        if results == "constraint_violation":
            sql = '''update income set income = ? where budget_id = ?'''
            results = self.write_sql(sql, [str(-100*float(entry.get_value())),self.budget_id])

    def load_accounts(self):
        if self.budget_active:
            accounts = self.read_temp()
            load_list = []
            for row in accounts:
                entry = position(row)
                vals = (db_in(entry.get_value()), entry.get_account(),self.budget_id)
                load_list.append(vals)
            sql = '''update budget_base 
                     set spending = ? 
                     where account_id = (select account_id from accounts where account_name = ?)
                     and budget_id = ?
                     '''
            account_write = self.write_sql(sql, load_list,single=False)
            if account_write == "sql_failure":
                print("Failed to update")
                exit(1)



    def open_budget(self):
        '''Create a new entry in the budgets table'''
        sql = '''insert into budgets (year,month) values (?, ?)'''
        budget_id = self.write_sql(sql,[str(self.year), str(self.month)],get_id=True)
        if budget_id == "constraint_violation":
            print("Budget already exists")
            return(3)
        elif budget_id == "sql_failure":
            print("Error encountered")
            exit(2)
        elif budget_id > 0:
            self.budget_id = budget_id
            self.check_budget_status()
            self.get_bean_accounts()
            self.load_accounts()
            self.insert_accounts()
            self.create_budget_envelopes()

    def create_budget_envelopes(self):
        sql = '''insert into budget_base 
                select ?, account_id, 0, 0, 0 from accounts'''
        results = self.write_sql(sql, [self.budget_id])
        
         
        
    def get_budget_id(self):
        sql = '''select budget_id from budgets where year = ? and month = ?'''
        budget_id = self.read_sql(sql,[str(self.year),str(self.month)],single=True)
        if budget_id == None:
            print("No budget for this month")
            exit(1)
        else:
            self.budget_id = budget_id[0]
        return(0)

    def get_account_id(self,name):
        sql = '''select account_id from accounts where account_name = ?'''
        result = self.read_sql(sql, name)
        return(result)

    def set_base_envelope(self,acct_id,value):
        sql = '''update budget_base
                set base_value = ?
                where budget_id = ?
                and account_id = ?'''
        results = self.write_sql(sql, [db_in(value), self.budget_id, acct_id])
        if results == 0:
            return(0)

    def redistribute_envelopes(self,acc1,acc2,transfer):
        sql = '''update budget_base
                 set base_value = base_value + ?
                 where budget_id = ?
                 and account_id = ?'''
        env1 = (-1*db_in(transfer), self.budget_id,acc1)
        env2 = (db_in(transfer), self.budget_id,acc2)
        results = self.write_sql(sql, [env1,env2],single=False)
        if results == 0:
            return(0)

    def make_correction(self,acc1,acc2,transfer):
        sql = '''insert into corrections
                 values (?, ?, ?, ?)'''
        env1 = (self.budget_id,acc1,'A',-transfer)
        env2 = (self.budget_id,acc2,'A',transfer)
        results = self.write_sql(sql, [env1,env2],single=False)
        if results == 0:
            return(0)

    def single_correction(self,acct_id,amount):
        sql = '''insert into corrections
                 values (?,?,?,?)'''
        vals = [self.budget_id,acct_id,'S',db_in(amount)]
        results = self.write_sql(sql, vals)
        if results == 0:
            return(0)
        else:
            return(8)

    def set_target(self,acct,targ_val):
        sql = '''update budget_base
                 set target = ?
                 where budget_id = ?
                 and account_id = ?'''
        results = self.write_sql(sql,[db_in(targ_val),self.budget_id,acct])
        if results == 0:
            return(0)
        else:
            return(9)

    def get_income(self):
        '''Read income from current month's budget'''
        sql = '''select income from income where budget_id = ?'''
        results = self.read_sql(sql,[self.budget_id], single=True)
        self.income = results[0]

    def allocation_balance(self):
        sql = '''select (i.income - (select sum(base_value) 
                                     from budget_base
                                     where budget_id = ?))
                 from income as i
                 where i.budget_id = ?'''
        results = self.read_sql(sql, [self.budget_id, self.budget_id], single=True)
        return(results[0])

    def base_planner(self):
        balance = db_out(self.allocation_balance())

        header = '''Income:  {}\nBalance: {}

                    '''
        print(header.format(db_out(self.income), self.text_color(balance)))

        sql = '''select a.account_id, 
                        a.account_name, 
                        b.target, 
                        c.correction_value, 
                        b.base_value
                 from accounts a, budget_base b, corrections c
                 where a.account_id = b.account_id
                 and b.account_id = c.account_id
                 and b.budget_id = c.budget_id
                 and b.budget_id = ?
                 and c.correction_type = ?
                 order by account_name'''

        results = self.read_sql(sql, [self.budget_id, 'C'])#,debug=True)

        if results != "sql_failure":
            table_values = []
            for i in results:
                table_values.append([i[0], i[1], db_out(i[2]), db_out(i[3]), self.text_color(db_out(i[4]))])
            print(tabulate(table_values, ["ID", "Account", "Target", "Carried", "Allocated"], tablefmt="simple"))


                

    def envelope_balance(self,carry=True):
        sql = '''
                create temporary table correction_temp 
                as select account_id, sum(correction_value) as total_correction 
                from corrections 
                where budget_id = ? 
                group by (account_id);
                '''
        results = self.write_sql(sql, [self.budget_id])
        sql = '''
                select a.account_id, a.account_name, 
                    b.base_value, c.total_correction, b.spending,
                    (b.base_value + c.total_correction - b.spending) as envelope_balance,
                    b.target
                from accounts a, budget_base b, correction_temp c
                where a.account_id = b.account_id
                and b.account_id = c.account_id
                and budget_id = ?
                order by account_name
                '''
        results = self.read_sql(sql, [self.budget_id])
        return(results)

    def return_balances(self,html=False):
        results = self.envelope_balance()
        t = lambda x: " " if x == 0 else db_out(x)

        if results != "sql_failure":
            table_values = []
            if html:
                for i in results:
                    table_values.append([i[0], i[1],t(i[6]),i[4],i[5]])
                document = '''<!DOCTYPE html><html>
                                <head>
                                <style>table, th, td{border:1px solid black; border-collapse: collapse};</style></head>
                                <body>
                                <h3>Balances</h3>
                                <table style='width 100%'>
                                <tr><th>ID</th><th>Account</th><th>Target</th><th>Spend</th><th>Balance</th></tr>
                                '''

                for row in table_values:
                    #row_format = "<tr><td>{}</td><td>{}</td><td>{}</td></tr>\n"
                    row_format = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td>{}</tr>\n"
                    #document += row_format.format(row[0], row[1], self.html_emph(row[2]))
                    document += row_format.format(row[0], row[1], t(row[2]),db_out(row[3]),self.text_color(db_out(row[4]), html=True))
                document += "</table></body></html>"
                f = open(html, 'w')
                f.write(document)
                f.close()

            else:
                for i in results:
                    table_values.append([i[0], i[1], t(i[6]),db_out(i[4]),self.text_color(db_out(i[5]))])
                print(tabulate(table_values, ["ID", "Account","Target", "Spend","Balance"], tablefmt="simple"))


    def text_color(self,txt,html=False):
        value = float(txt)
        if value > 0:
            color = "green"
        elif value == 0:
            color = "blue"
        else:
            color = "red"
        if html==True:
            return("<td style=color:{}>{}</td>".format(color,txt))
        else:
            return(colored(txt, color))

    def html_emph(self,txt):
        value = float(txt)
        if value > 0:
            fmt_string = "<b>{}</b>"
        elif value == 0:
            fmt_string = "{}"
        else:
            fmt_string = "<em>{}</em>"
        return(fmt_string.format(txt))
        
    def activate_budget(self):
        if self.budget_active:
            print("Budget already active")
            exit(4)
        else:
            balance = self.allocation_balance()
            if balance != 0:
                print("Envelope allocations do not balance income")
                exit(1)
            else:
                sql = '''update budgets set active = 1
                         where budget_id = ?'''
                result = self.write_sql(sql, [self.budget_id])

    def deactivate_budget(self):
        current_id = self.budget_id
        sql = '''update budgets set active = 0, closed = 1
                 where budget_id = ?'''
        result = self.write_sql(sql, [current_id])
        if result == 0:
            balances = self.envelope_balance()
            self.month = self.next_month
            self.year = self.next_year
            status = self.open_budget()
            if status == 3:
                self.get_budget_id()
            carry_list = []
            for i in balances:
                carry_list.append([self.budget_id,i[0],'C',i[5]])
            sql = '''insert into corrections values (?,?,?,?)'''
            results = self.write_sql(sql, carry_list, single=False)
            if results == 0:
                print("Budget Closed")
                exit(0)
            else:
                print("Error")
                exit(0)

    def check_budget_status(self):
        sql = '''select active,closed from budgets where budget_id = ?'''
        result = self.read_sql(sql, [self.budget_id], single=True)
        if result[0] == 1:
            self.budget_active = True
        elif result[0] == 0:
            self.budget_active = False
        if result[1] == 1:
            self.budget_closed = True
        else:
            self.budget_closed = False

    def copy_allocations(self, allocations="base", targets=True):
        if allocations == "base":
            alloc = "base_value"
        elif allocations == "spend":
            alloc = "spending"

        #TODO include corrected budget as an option

        if targets:
            targ = ",target "
        else:
            targ = ""

        sql = "select account_id," + alloc + targ + "from budget_base where budget_id = ?"
        current_id = self.budget_id
        self.month = self.last_month
        self.year = self.last_year

        self.get_budget_id()

        results = self.read_sql(sql, [self.budget_id])

        copy_list = []
        if len(targ) > 0:
            sql = '''update budget_base
                     set base_value = ?,
                         target = ?
                     where budget_id = ?
                     and account_id = ?'''
            for i in results:
                copy_list.append([i[1], i[2], current_id,i[0]])
        else:
            sql = '''update budget_base
                     set base_value = ?
                     where budget_id = ?
                     and account_id = ?'''
            for i in results:
                copy_list.append([i[1], current_id,i[0]])

        results = self.write_sql(sql,copy_list,single=False)
        if results == 0:
            return(0)
        else:
            return(6)


def main():
    config = ConfigParser()
    config.read(config_file)

    db = os.path.expandvars(config.get("DEFAULT", "db"))
    beanfile = os.path.expandvars(config.get("DEFAULT", "beanfile"))
    tempfile = os.path.expandvars(config.get("DEFAULT", "tempfile"))

    parser = argparse.ArgumentParser(description="Manage budgets based on beancount file data")
    parser.add_argument("-m", action="store", dest="month", default=None, help="Set budget month")
    parser.add_argument("-y", action="store", dest="year", default=None, help="Set budget year")
    parser.add_argument("-e", action="store_true", dest="edit", default=False, help="Edit base budget allocations")
    parser.add_argument("-a", action="store_true", dest="adjust", default=False, help="Adjust envelope balances")
    parser.add_argument("-b", action="store_true", dest="budget_init", default=False, help="Initialise a new budget month")
    parser.add_argument("-H", action="store", dest="html_dest", default=None,help="Save balances to html file")
    parser.add_argument("-A", action="store_true", dest="activate", default=False, help="Activate a budget")
    parser.add_argument("-D", action="store_true", dest="deactivate", default=False, help="Deactivate budget")
    parser.add_argument("-c", action="store_true", dest="copy", default=False, help="Copy base budget values from last month")
    parser.add_argument("-s", action="store_true", dest="single_correction", default=False,help="Apply a single account correction")
    parser.add_argument("-t", action="store_true", dest="set_target", default=False,help="Set an account target value")
    #parser.add_argument("-u", action="store_true", dest="update", default=False,help="Update budget with new expense accounts (NOT IMPLEMENTED)")

    args = parser.parse_args()

    if args.budget_init:
        b = budget(db, beanfile,tempfile,args.month,args.year,init=True)
        exit()
    else:
        b = budget(db, beanfile,tempfile,args.month,args.year)
    
        if args.activate:
            if b.budget_closed:
                print("Budget cannot be reactivated")
                exit(11)
            elif b.budget_active:
                print("Budget already active")
                exit(12)
            else:
                b.activate_budget()

        elif args.deactivate:
            if b.budget_closed:
                print("Budget already deactivated")
                exit(13)
            elif not b.budget_active:
                print("Budget is not active")
                exit(14)
            b.deactivate_budget()

        # Adjustment envelopes
        elif args.adjust:
            if not b.budget_active:
                print("Adjustments cannot be applied to inactive budgets")
                exit(10)
            print("\033[H\033[J")
            b.return_balances()
            print("\n")
            print("Adjust Balances\n")
            from_acct = input("Account to take from: ")
            if len(from_acct) == 0:
                print("Cancelling...")
                exit(0)
            to_acct = input("Account to add to: ")
            if len(to_acct) == 0:
                print("Cancelling...")
                exit(0)
            adjust_val = input("Amount to move: ")
            if len(adjust_val) == 0:
                print("Cancelling...")
                exit(0)
            result = b.redistribute_envelopes(int(from_acct), int(to_acct), float(adjust_val))
            if result == 0:
                print("\033[H\033[J")
                print("Correction applied")
                b.return_balances()

        elif args.single_correction:
            if not b.budget_active:
                print("Adjustments cannot be applied to inactive budgets")
                exit(10)
            print("\033[H\033[J")
            b.return_balances()
            print("\n")
            print("Single Account Adjustment\n")
            acct = input("Account to Adjust: ")
            if len(acct) == 0:
                print("Cancelling...")
                exit(0)
            adjust_val = input("Adjustment Amount: ")
            if len(adjust_val) == 0:
                print("Cancelling...")
                exit(0)
            result = b.single_correction(acct,adjust_val)
            if result == 0:
                print("\033[H\033[J")
                print("Correction applied")
                b.return_balances()

        elif args.set_target:
            if b.budget_closed:
                print("Targets cannot be adjusted after budget closed")
                exit(11)
            print("\033[H\033[J")
            b.base_planner()
            print("\n")
            print("Set target value\n")
            acct = input("Target Account: ")
            if len(acct) == 0:
                print("Cancelling...")
                exit(0)
            targ_val = input("Target Amount: ")
            if len(targ_val) == 0:
                print("Cancelling...")
                exit(0)
            result = b.set_target(acct,targ_val)
            if result == 0:
                print("\033[H\033[J")
                print("Target set")
                b.base_planner()

        elif args.edit:
            if b.budget_active:
                print("Budget is already active")
                exit(4)
            elif b.budget_closed:
                print("Budget is closed")
                exit(5)
            while True:
                print("\033[H\033[J")
                b.base_planner()
                selected_acct = input("Account to budget: ")
                if len(selected_acct) == 0:
                    print("Cancelling...")
                    exit(0)
                budget_value = input("Allocated amount: ")
                if len(budget_value) == 0:
                    print("Cancelling...")
                    exit(0)
                b.set_base_envelope(int(selected_acct), budget_value)

        elif args.html_dest:
            b.return_balances(html=args.html_dest)

        elif args.copy:
            alloc = input("Copy (b)ase values or (s)pending? [b]: ")
            if alloc == "s":
                allocation = "spend"
            elif alloc == "b" or alloc == "":
                allocation = "base"
            else:
                print("Invalid selection")
                exit(7)
            targ = input("Copy targets? [y]: ")
            if targ == "n":
                targets = False
            elif targ == "y" or targ == "":
                targets = True
            else:
                print("Invalid selection")
                exit(7)
            result = b.copy_allocations(allocation,targets)
            if result == 0:
                print("Base values updated")
            else:
                print("Error")

        #elif args.update:
        #    b.update_missing()

            
        else:
            print("\033[H\033[J")
            if (not b.budget_active) and (not b.budget_closed):
                b.base_planner()
            else:
                b.return_balances()
            print("\n")

if __name__ == "__main__":
    main()
