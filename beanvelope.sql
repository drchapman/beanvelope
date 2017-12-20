PRAGMA foreign_keys=ON;

-- budget table
create table budgets
	(budget_id integer primary key,
	year integer,
	month integer,
	active boolean default 0,
	closed boolean default 0
);

-- accounts table
create table accounts
	(account_id integer primary key,
	account_name text,
	closed boolean default 0
);

-- income table
create table income
	(budget_id integer,
	income number,
	foreign key(budget_id) references budgets(budget_id)
);

-- base budgeted values
create table budget_base
	(budget_id integer,
	account_id integer,
	base_value number default 0.00,
	target number default 0.00,
	spending number default 0.00,
	foreign key(budget_id) references budgets(budget_id),
	foreign key(account_id) references accounts(account_id)
);

-- corrections table
create table corrections
	(budget_id integer,
	account_id integer,
	correction_type char,
	correction_value number,
	foreign key(budget_id) references budgets(budget_id),
	foreign key(account_id) references accounts(account_id)
);

-- modifications for individual queries
create table filter_mods
	(account_id integer,
	filter_text text,
	foreign key(account_id) references accounts(account_id)
)

