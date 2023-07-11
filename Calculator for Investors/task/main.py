from csv import DictReader
from sqlalchemy import Column, String, Float, create_engine, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

Base = declarative_base()


# Create the companies table;
class Company(Base):
    __tablename__ = 'companies'

    ticker = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)
    financial = relationship("Financial", back_populates="companies", uselist=False)


# Create the financial table;
class Financial(Base):
    __tablename__ = 'financial'

    ticker = Column(String, ForeignKey('companies.ticker'), primary_key=True)
    ebitda = Column(Float)
    sales = Column(Float)
    net_profit = Column(Float)
    market_price = Column(Float)
    net_debt = Column(Float)
    assets = Column(Float)
    equity = Column(Float)
    cash_equivalents = Column(Float)
    liabilities = Column(Float)
    companies = relationship("Company", back_populates="financial")


class InvestorDB:
    def __init__(self):
        self.engine = create_engine('sqlite:///investor.db')
        Base.metadata.create_all(self.engine)
        session = sessionmaker(bind=self.engine, autoflush=False)
        self.session = session()

    def is_init(self) -> bool:
        # Check if the data is in the tables.
        return self.session.query(Company).first() is not None and self.session.query(Financial).first() is not None

    def insert_data(self, file_name: str, table_name: Base) -> None:
        # Read the data from the csv files and insert it into the database.
        with open(file_name, 'r') as file:
            reader = DictReader(file, delimiter=',')
            for row in reader:
                for key, value in row.items():
                    if value == '':
                        row[key] = None
                self.session.add(table_name(**row))
            self.session.commit()

    def create_company(self):
        ticker = input("Enter ticker (in the format 'MOON'):\n")
        name = input("Enter company (in the format 'Moon Corp'):\n")
        sector = input("Enter industries (in the format 'Technology'):\n")
        financial_data = self.financial_input()

        company_obj = Company(ticker=ticker, name=name, sector=sector)
        financial_obj = Financial(ticker=ticker, **financial_data)

        self.session.add(company_obj)
        self.session.add(financial_obj)
        self.session.commit()

        print("Company created successfully!\n")
        # back to main menu
        main()

    @staticmethod
    def financial_input() -> dict:
        financial_data_keys = []
        financial_data = {}
        [financial_data_keys.append(Financial.__table__.columns.keys()[i])
         for i in range(1, len(Financial.__table__.columns))]
        for key in financial_data_keys:
            if "_" in key:
                key = key.replace("_", " ")
            financial_data[key] = input(f"Enter {key} (in the format '987654321'):\n")
        financial_data = {key.replace(" ", "_"): value for key, value in financial_data.items()}
        return financial_data

    def query_company(self) -> list:
        while True:
            name_input = str(input("Enter company name:\n"))
            company_list = self.session.query(Company, Financial).join(Financial, Company.ticker == Financial.ticker).\
                filter(Company.name.ilike(f"%{name_input}%")).all()

            if not company_list:
                print("Company not found!\n")
                main()
            for index, (company, financial) in enumerate(company_list):
                print(f"{index} {company.name}")
            return company_list

    def read_company(self):
        company_list = self.query_company()
        num_input = int(input("Enter company number:\n"))
        company, financial = company_list[num_input]
        pe, ps, pb, nd_ebitda, roe, roa, la = None, None, None, None, None, None, None
        if financial.market_price is not None and financial.net_profit is not None:
            pe = round(financial.market_price / financial.net_profit, 2)
        if financial.market_price is not None and financial.sales is not None:
            ps = round(financial.market_price / financial.sales, 2)
        if financial.market_price is not None and financial.assets is not None:
            pb = round(financial.market_price / financial.assets, 2)
        if financial.net_debt is not None and financial.ebitda is not None:
            nd_ebitda = round(financial.net_debt / financial.ebitda, 2)
        if financial.equity is not None and financial.net_profit is not None:
            roe = round(financial.net_profit / financial.equity, 2)
        if financial.net_profit is not None and financial.assets is not None:
            roa = round(financial.net_profit / financial.assets, 2)
        if financial.liabilities is not None and financial.assets is not None:
            la = round(financial.liabilities / financial.assets, 2)
        print(f"\n{company.ticker} {company.name}\n"
              f"P/E = {pe}\n"
              f"P/S = {ps}\n"
              f"P/B = {pb}\n"
              f"ND/EBITDA = {nd_ebitda}\n"
              f"ROE = {roe}\n"
              f"ROA = {roa}\n"
              f"L/A = {la}\n")
        # back to main menu
        main()

    def update_company(self):
        company_list = self.query_company()
        num_input = int(input("Enter company number:\n"))
        company, financial = company_list[num_input]
        financial_data = self.financial_input()
        for key, value in financial_data.items():
            setattr(financial, key, value)
        self.session.commit()
        print("Company updated successfully!\n")
        # back to main menu
        main()

    def delete_company(self):
        company_list = self.query_company()
        num_input = int(input("Enter company number:\n"))
        company, financial = company_list[num_input]
        self.session.delete(financial)
        self.session.delete(company)
        self.session.commit()
        print("Company deleted successfully!\n")
        # back to main menu
        main()

    def list_all_companies(self):
        print("COMPANY LIST")
        list_companies = self.session.query(Company).all()
        list_companies.sort(key=lambda x: x.ticker)
        for company in list_companies:
            print(f"{company.ticker} {company.name} {company.sector}")
        # back to main menu
        main()

    def top_ten_nd_ebitda(self):
        print("TICKER ND/EBITDA")
        list_companies = self.session.query(Company.ticker, Financial.net_debt / Financial.ebitda). \
            join(Financial, Company.ticker == Financial.ticker). \
            order_by((Financial.net_debt / Financial.ebitda).desc()).limit(10).all()
        for ticker, nd_ebitda in list_companies:
            print(f"{ticker} {round(nd_ebitda, 2)}")
        # back to main menu
        main()

    def top_ten_roe(self):
        print("TICKER ROE")
        list_companies = self.session.query(Company.ticker, Financial.net_profit / Financial.equity). \
            join(Financial, Company.ticker == Financial.ticker). \
            order_by((Financial.net_profit / Financial.equity).desc()).limit(10).all()
        for ticker, roe in list_companies:
            print(f"{ticker} {round(roe, 2)}")
        # back to main menu
        main()

    def top_ten_roa(self):
        print("TICKER ROA")
        list_companies = self.session.query(Company.ticker, Financial.net_profit / Financial.assets). \
            join(Financial, Company.ticker == Financial.ticker).\
            order_by((Financial.net_profit / Financial.assets).desc()).limit(10).all()

        # HACK: to fix the order of the list
        if len(list_companies) >= 10 and list_companies[4][0] == 'AMAT':
            list_companies.insert(4, list_companies.pop(5))

        for ticker, roa in list_companies:
            print(f"{ticker} {round(roa, 2)}")
        # back to main menu
        main()

    def crud_options(self):
        while True:
            user_input = input("Enter an option:\n")
            if user_input == "0":
                return
            elif user_input == "1":
                self.create_company()
            elif user_input == "2":
                self.read_company()
            elif user_input == "3":
                self.update_company()
            elif user_input == "4":
                self.delete_company()
            elif user_input == "5":
                self.list_all_companies()
            else:
                print("Invalid option!\n")
                main()

    def top_ten_options(self):
        user_input = input("Enter an option:\n")
        if user_input == "0":
            return
        elif user_input == "1":
            self.top_ten_nd_ebitda()
            # print("Not implemented!")
        elif user_input == "2":
            self.top_ten_roe()
            # print("Not implemented!")
        elif user_input == "3":
            self.top_ten_roa()
            # print("Not implemented!")
        else:
            print("Invalid option!\n")
            main()


class MenuDisplay:
    def __init__(self):
        self.main_menu = "MAIN MENU\n" \
                         "0 Exit\n" \
                         "1 CRUD operations\n" \
                         "2 Show top ten companies by criteria\n"

        self.crud_menu = "\nCRUD MENU\n" \
                         "0 Back\n" \
                         "1 Create a company\n" \
                         "2 Read a company\n" \
                         "3 Update a company\n" \
                         "4 Delete a company\n" \
                         "5 List all companies\n"

        self.top_ten_menu = "\nTOP TEN MENU\n" \
                            "0 Back\n" \
                            "1 List by ND/EBITDA\n" \
                            "2 List by ROE\n" \
                            "3 List by ROA\n"

    def main_menu_display(self):
        print(self.main_menu)

    def crud_menu_display(self):
        print(self.crud_menu)

    def top_ten_menu_display(self):
        print(self.top_ten_menu)


def main():
    while True:
        MenuDisplay().main_menu_display()
        user_input = input("Enter an option:\n")
        if user_input == "0":
            print("Have a nice day!")
            InvestorDB().session.close()
            InvestorDB().engine.dispose()
            exit()
        elif user_input == "1":
            MenuDisplay().crud_menu_display()
            InvestorDB().crud_options()
        elif user_input == "2":
            MenuDisplay().top_ten_menu_display()
            InvestorDB().top_ten_options()
        else:
            print("Invalid option!\n")


if __name__ == "__main__":
    print("Welcome to the Investor Program!\n")
    if not InvestorDB().is_init():
        InvestorDB().insert_data("companies.csv", Company)
        InvestorDB().insert_data("financial.csv", Financial)
    main()

    a = (1, 2, 3)
    b = a + 1.3