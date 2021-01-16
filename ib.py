import configparser
import datetime
import os
import sys
from collections import defaultdict
from glob import glob
from typing import Dict, List, NamedTuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from docxtpl import DocxTemplate

config = configparser.ConfigParser()
config.read("settings.ini")
settings = config["settings"]
DIRNAME = settings.get("dirname")
START_DATE = settings.get("start_date")

SECTIONS = [
    "Deposits & Withdrawals",
    "Trades",
    "Fees",
    "Dividends",
    "Withholding Tax",
    "Change in Dividend Accruals",
    "Interest",
]

CURRENCIES = {
    "USD": "R01235",
    "EUR": "R01239",
    "PLN": "R01565",
}

MARKET_DATA = {
    "USD": None,
    "EUR": None,
    "PLN": None,
}

# TODO get currencies


class Asset(NamedTuple):
    date: datetime.date
    price: float
    fee: float
    currency: str


def get_currency_market_data():
    for currency, code in CURRENCIES.items():
        print(f"Получение таблицы курса {currency}...")
        format_1 = "%d.%m.%Y"
        format_2 = "%m.%d.%Y"
        from_ = datetime.datetime.strptime(START_DATE, format_1)
        to = datetime.datetime.today()
        url = "https://cbr.ru/Queries/UniDbQuery/DownloadExcel/98956"
        params = {
            "Posted": "True",
            "mode": "1",
            "VAL_NM_RQ": code,
            "From": from_.strftime(format_1),
            "To": to.strftime(format_1),
            "FromDate": from_.strftime(format_2),
            "ToDate": to.strftime(format_2),
        }
        response = requests.get(url, params=params)
        df = pd.read_excel(response.content).rename(columns={"data": "date", "curs": "val"})
        assert df.shape[0] > 0, f"Не удалось загрузить таблицу курсов {currency}!"
        MARKET_DATA[currency] = df
        with open(f"{currency}.xlsx", "wb") as file:
            file.write(response.content)


def split_report(year):
    print("Разделение отчета на разделы...")
    file_name = f"{year}.csv"
    if not os.path.exists(file_name):
        input(f"Не найден файл отчета за {year}г. ({file_name})")
        sys.exit()

    if not os.path.exists(DIRNAME):
        os.mkdir(DIRNAME)
    for old_file in glob(os.path.join(DIRNAME, f"{year}*.csv")):
        os.remove(old_file)

    out_file = None
    with open(file_name) as f:
        while True:
            line = f.readline()
            if not line:
                break

            section, header, *_ = line.split(",")
            if header == "Header":
                if out_file:
                    out_file.close()
                    out_file = None
                if section in SECTIONS:
                    out_file_name = os.path.join(DIRNAME, f"{year}_{section}.csv")
                    if os.path.exists(out_file_name):  # if second header in the same section - skip header
                        out_file_name = out_file_name.replace(".csv", f"{f.tell()}.csv")
                    out_file = open(out_file_name, "w")
                    assert out_file, f"Can't open file {out_file_name}!"
            if out_file and section in SECTIONS:
                out_file.write(line)

    if out_file:
        out_file.close()


def get_ticker_price(ticker: str) -> float:
    return float(yf.Ticker(ticker).history(period="1d").Close.median())


def load_data(year):
    print("Чтение разделов отчета...")
    data = {}
    for file_path in glob(os.path.join(DIRNAME, "*.csv")):
        if int(os.path.basename(file_path).split("_")[0]) > year:
            continue

        print(f"--{file_path}")
        df = pd.read_csv(file_path, thousands=",")
        section = df.iloc[0, 0]
        if section not in data:
            data[section] = df
        else:
            df.columns = data[section].columns
            data[section] = data[section].append(df, ignore_index=True)

    if "Deposits & Withdrawals" in data:
        cash_flow = data["Deposits & Withdrawals"]
        cash_flow.columns = [col.lower() for col in cash_flow]
        cash_flow = cash_flow.rename(columns={"settle date": "date"})
        cash_flow = cash_flow[cash_flow.header == "Data"]
        cash_flow = pd.DataFrame(cash_flow[cash_flow.currency.isin(CURRENCIES)])
        cash_flow.date = pd.to_datetime(cash_flow.date)
    else:
        cash_flow = None

    if "Trades" in data:
        trades = data["Trades"]
        trades.columns = [col.lower() for col in trades]
        trades = trades.rename(
            columns={
                "comm/fee": "fee",
                "date/time": "date",
                "t. price": "price",
                "comm in usd": "fee",
            }
        )
        trades = trades[trades.header == "Data"]
        trades = trades[trades.fee < 0]
        trades.date = pd.to_datetime(trades.date)
    else:
        trades = None

    if "Fees" in data:
        commissions = data["Fees"]
        commissions.columns = [col.lower() for col in commissions]
        commissions = commissions[commissions.header == "Data"]
        commissions = commissions[commissions.subtitle != "Total"]
        commissions.date = pd.to_datetime(commissions.date)
        commissions = commissions[commissions.date.dt.year == year]
    else:
        commissions = None

    if "Interest" in data:
        interests = data["Interest"]
        interests.columns = [col.lower() for col in interests]
        interests = interests[interests.header == "Data"]
        interests = interests[interests.currency != "Total"]
        interests.date = pd.to_datetime(interests.date)
        interests = interests[interests.date.dt.year == year]
    else:
        interests = None

    if "Dividends" in data:
        div = data["Dividends"]
        div.columns = [col.lower() for col in div]
        div = pd.DataFrame(div[div.currency.isin(CURRENCIES)])
        div.date = pd.to_datetime(div.date)
        div = pd.DataFrame(div[div.date.dt.year == year])
    else:
        div = None

    if div is not None and "Withholding Tax" in data:
        div_tax = data["Withholding Tax"]
        div_tax.columns = [col.lower() for col in div_tax]
        div_tax = pd.DataFrame(div_tax[div_tax.currency.isin(CURRENCIES)])
        div_tax.date = pd.to_datetime(div_tax.date)
        div_tax = pd.DataFrame(div_tax[div_tax.date.dt.year == year])
        div.description = [desc.split(" Cash Dividend")[0].replace(" (", "(") for desc in div.description]
        div_tax.description = [desc.split(" Cash Dividend")[0].replace(" (", "(") for desc in div_tax.description]
        div.rename(columns={"description": "ticker"}, inplace=True)
        div_tax.rename(columns={"description": "ticker"}, inplace=True)

        if div.shape[0] != div_tax.shape[0]:
            print("Размеры таблиц дивидендов и налогов по ним не совпадают. Попробуем исправить...")
            df = pd.DataFrame(columns=div_tax.columns)
            for index, row in div.iterrows():
                tax_row = div_tax[(div_tax["date"] == row["date"]) & (div_tax["ticker"] == row["ticker"])]
                if not tax_row.empty:
                    df.loc[index] = tax_row.T.squeeze()
                else:
                    df.loc[index] = pd.Series(
                        {
                            "withholding tax": "Withholding Tax",
                            "header": "Data",
                            "currency": div["currency"],
                            "date": div["date"],
                            "ticker": row["ticker"],
                            "amount": np.rint(0),
                            "code": np.nan,
                        }
                    )
            div_tax = df
    else:
        div_tax = None

    if "Change in Dividend Accruals" in data:
        div_accruals = data["Change in Dividend Accruals"]
        div_accruals.columns = [col.lower() for col in div_accruals]
        div_accruals = pd.DataFrame(div_accruals[div_accruals.currency.isin(CURRENCIES)])
        div_accruals.date = pd.to_datetime(div_accruals.date)
        div_accruals = pd.DataFrame(div_accruals[div_accruals.date.dt.year == year])
    else:
        div_accruals = None

    return cash_flow, trades, commissions, div, div_tax, div_accruals, interests


def add_transactions() -> List[Dict]:
    print("Введите сделки в формате {тикер} {кол-во} {валюта}")
    print("Отрицательное количество означает продажу")
    print("Пример покупки: TSLA 400 USD")
    print("Пример продажи: TSLA -400 USD")
    print("Введите 'r' для того чтобы начать заново или 'q' для завершения.")

    res = []
    i = 0
    while True:
        inp = input(f"{i + 1}: ")
        if inp == "r":
            res.clear()
            i = 0
            print("Сброс...")
            continue

        if inp == "q":
            break

        values = inp.split(" ")
        if len(values) != 3:
            print("Не распознан ввод, пример: TSLA 400 USD")
            continue

        ticker = values[0].upper()

        count = float(values[1])
        if count == 0:
            print("Количество не может быть равным нулю.")
            continue

        currency = values[2].upper()
        if currency not in CURRENCIES:
            print(f"Валюта должна быть одна из поддерживаемых: {list(CURRENCIES)}")
            continue

        price = get_ticker_price(ticker)
        res.append(
            {
                "currency": currency,
                "symbol": ticker,
                "fee": -1.0,
                "date": datetime.datetime.today(),
                "quantity": count,
                "price": price,
            }
        )
        word = "Куплено" if count > 0 else "Продано"
        print(f"{word}: {ticker} {abs(count)}шт по цене {price}{currency}")

        i += 1
    print(f"Добавлено {len(res)} сделок.")
    return res


def get_currency(date: datetime.date, cur: str) -> float:
    if cur == "RUB":
        return 1

    assert cur in MARKET_DATA, f"Неизвестная валюта {cur}!"
    data = MARKET_DATA[cur]
    diff = data.date - date
    index_max = diff[(diff <= pd.to_timedelta(0))].idxmax()
    return float(data.iloc[[index_max]].val)


def calculate_cash_flow(cash_flow):
    print("Расчет таблицы переводов...")
    res = cash_flow[["date", "currency", "amount"]].copy()
    res["type"] = ["Перевод на счет" if amount > 0 else "Снятие со счета" for amount in cash_flow.amount]
    cash_flow_rub_sum = res[res.currency == "RUB"].amount.sum().round(2)
    cash_flow_usd_sum = res[res.currency == "USD"].amount.sum().round(2)
    cash_flow_eur_sum = res[res.currency == "EUR"].amount.sum().round(2)
    return res, cash_flow_rub_sum, cash_flow_usd_sum, cash_flow_eur_sum


def calculate_dividends(div, div_tax):
    print("Расчет таблицы дивидендов...")
    res = pd.DataFrame()
    res["ticker"] = div["ticker"].values
    res["date"] = div["date"].values
    res["amount"] = div["amount"].values.round(2)
    res["currency"] = div["currency"].values
    if div_tax is not None:
        res["tax_paid"] = [-value.round(2) if value < 0 else value for value in div_tax["amount"].values]
    else:
        res["tax_paid"] = 0
    res["cur_price"] = [get_currency(row.date, row.currency) for _, row in div.iterrows()]
    res["amount_rub"] = (res.amount * res.cur_price).round(2)
    res["tax_paid_rub"] = (res.tax_paid * res.cur_price).round(2)
    res["tax_full_rub"] = (res.amount_rub * 0.13).round(2)
    res["tax_rest_rub"] = (res.tax_full_rub - res.tax_paid_rub).round(2).clip(lower=0)
    return res


def calculate_dividend_accruals(div_accruals):
    print("Расчет таблицы корректировки дивидендов...")
    res = pd.DataFrame()
    res["ticker"] = div_accruals["symbol"]
    res["date"] = div_accruals["date"]
    res["amount"] = div_accruals["gross amount"].round(2)
    res["currency"] = div_accruals["currency"].values
    res["tax_paid"] = div_accruals["tax"].round(2)
    res["cur_price"] = [get_currency(row.date, row.currency) for _, row in div_accruals.iterrows()]
    res["amount_rub"] = (res.amount * res.cur_price).round(2)
    res["tax_paid_rub"] = (res.tax_paid * res.cur_price).round(2)
    res["tax_full_rub"] = (res.amount_rub * 0.13).round(2)
    res["tax_rest_rub"] = (res.tax_full_rub - res.tax_paid_rub).round(2)
    return res


def fees_calc(commissions):
    print("Расчет таблицы комиссий...")
    fees = pd.DataFrame()
    fees["date"] = commissions.date
    fees["fee"] = -commissions.amount
    fees["currency"] = commissions["currency"].values
    fees["cur_price"] = [get_currency(row.date, row.currency) for _, row in commissions.iterrows()]
    fees["fee_rub"] = (fees.fee * fees.cur_price).round(2)
    return fees


def calculate_trades(trades, year):
    print("Расчет таблицы сделок...")
    all_assets = defaultdict(list)
    rows = []
    # TODO norm sdelay
    for ticker, val in trades.groupby("symbol"):
        fail = False
        for date, price, fee, quantity, currency in zip(val.date, val.price, val.fee, val.quantity, val.currency):
            if fail:
                break
            for _ in range(int(abs(quantity))):
                if fail:
                    break
                if quantity > 0:
                    all_assets[ticker].append(Asset(date, price, fee, currency))
                elif quantity < 0:
                    if all_assets[ticker]:
                        buy_date, buy_price, buy_fee, buy_currency = all_assets[ticker].pop(0)
                        if date.year == year:
                            # buy
                            rows.append(
                                {
                                    "ticker": ticker,
                                    "date": buy_date,
                                    "price": buy_price,
                                    "fee": buy_fee,
                                    "cnt": 1,
                                    "currency": buy_currency,
                                }
                            )
                            # sell
                            rows.append(
                                {
                                    "ticker": ticker,
                                    "date": date,
                                    "price": price,
                                    "fee": fee,
                                    "cnt": -1,
                                    "currency": currency,
                                }
                            )
                    else:
                        print(
                            f"Актив ({ticker}) продан в большем количестве, чем покупался. "
                            "Операции SHORT не поддерживаются."
                        )
                        rows = [row for row in rows if row["ticker"] != ticker]
                        fail = True

    if datetime.datetime.today().year == year and input("Рассчитать налоговые оптимизации? [y/n]: ") == "y":
        print("Расчет налоговых оптимизаций...")
        for ticker, assets in all_assets.items():
            if ticker in CURRENCIES:
                continue
            price_today = get_ticker_price(ticker)
            total = 0
            count = 0
            for asset in assets:
                cur_date_price = get_currency(asset.date, asset.currency)
                cur_now_price = get_currency(datetime.datetime.today(), asset.currency)
                result = -asset.price * cur_date_price + price_today * cur_now_price
                if result >= 0:
                    break

                count += 1
                total += result
            if total < 0:
                print(f"Можно продать {count} {ticker} и получить {abs(round(total, 2))}руб. бумажного убытка")
        print("\n")

    return pd.DataFrame(rows, columns=["ticker", "date", "price", "fee", "cnt", "currency"])


def calculate_interest(interests):
    print("Расчет таблицы по программе повышения доходности")
    interest_calc = pd.DataFrame()
    interest_calc["date"] = interests.date
    interest_calc["description"] = interests.description
    interest_calc["currency"] = interests.currency
    interest_calc["amount"] = interests.amount
    interest_calc["cur_price"] = [get_currency(row.date, row.currency) for _, row in interests.iterrows()]
    interest_calc["amount_rub"] = (interest_calc.amount * interest_calc.cur_price).round(2)
    interest_calc["rest"] = (interest_calc.amount * interest_calc.cur_price * 0.13).round(2)
    interest_calc = interest_calc.sort_values(["date"])
    return interest_calc


def main():
    year = int(input("Введите год отчета: "))

    get_currency_market_data()

    split_report(year)

    cash_flow, trades, commissions, div, div_tax, div_accruals, interests = load_data(year)

    if (
        trades is not None
        and datetime.datetime.today().year == year
        and input("Хотите добавить сделки купли или продажи? [y/n]: ") == "y"
    ):
        extra_transactions = add_transactions()
        for transaction in extra_transactions:
            trades = trades.append(transaction, ignore_index=True)

    # if cash_flow is not None:
    #     cash_flow_res, cash_flow_rub_sum, cash_flow_usd_sum, cash_flow_eur_sum = calculate_cash_flow(cash_flow)
    #     # print("\ncash_flow_res:")
    #     # print(cash_flow_res.head(2))
    #     # print(cash_flow_rub_sum, cash_flow_usd_sum, cash_flow_eur_sum)
    #     # print("\n")
    # else:
    #     print("Нет данных по переводам")
    #     cash_flow_res = None

    if div is not None:
        div_res = calculate_dividends(div, div_tax)
        div_sum = round(div_res.amount_rub.sum(), 2)
        div_tax_paid_rub_sum = round(div_res.tax_paid_rub.sum(), 2)
        div_tax_full_rub_sum = round(div_res.tax_full_rub.sum(), 2)
        div_tax_rest_sum = round(div_res.tax_rest_rub.sum(), 2)
    else:
        print("Нет данных по начисленным дивидендам")
        div_res = None
        div_tax_rest_sum = 0
        div_sum = 0
        div_tax_paid_rub_sum = 0
        div_tax_full_rub_sum = 0

    if div_accruals is not None:
        div_accruals_res = calculate_dividend_accruals(div_accruals)
        div_accruals_sum = div_accruals_res.amount_rub.sum().round(2)
        div_accruals_tax_paid_rub_sum = div_accruals_res.tax_paid_rub.sum().round(2)
        div_accruals_tax_full_rub_sum = div_accruals_res.tax_full_rub.sum().round(2)
        div_accruals_tax_rest_sum = div_accruals_res.tax_rest_rub.sum().round(2)
    else:
        print("Нет данных по изменениям в начислении дивидендов")
        div_accruals_res = None
        div_accruals_tax_rest_sum = 0
        div_accruals_sum = 0
        div_accruals_tax_paid_rub_sum = 0
        div_accruals_tax_full_rub_sum = 0

    div_final_tax_rest_sum = (div_tax_rest_sum + div_accruals_tax_rest_sum).round(2)
    div_final_sum = (div_sum + div_accruals_sum).round(2)
    div_tax_paid_final_sum = (div_tax_paid_rub_sum + div_accruals_tax_paid_rub_sum).round(2)
    div_tax_need_pay_final_sum = (div_tax_rest_sum + div_accruals_tax_rest_sum).round(2)

    if commissions is not None:
        fees_res = fees_calc(commissions)
        fees_rub_sum = fees_res.fee_rub.sum().round(2)
    else:
        print("Нет данных по комиссиям")
        fees_res = None
        fees_rub_sum = 0

    if trades is not None:
        trades_res = calculate_trades(trades, year)
        if len(trades_res):
            trades_res = trades_res.groupby(["ticker", "date", "price", "fee", "currency"], as_index=False)["cnt"].sum()
        trades_res["type"] = ["Покупка" if cnt > 0 else "Продажа" for cnt in trades_res.cnt]
        trades_res["price"] = trades_res.price.round(2)
        trades_res["fee"] = trades_res.fee.round(2) * -1
        trades_res["amount"] = (trades_res.price * trades_res.cnt * -1 - trades_res.fee).round(2)
        trades_res["cur_price"] = [get_currency(row.date, row.currency) for _, row in trades_res.iterrows()]
        trades_res["amount_rub"] = (trades_res.amount * trades_res.cur_price).round(2)
        trades_res["rest"] = (trades_res.amount * trades_res.cur_price * 0.13).round(2)
        trades_res["cnt"] = trades_res.cnt.abs()
        trades_res = trades_res.sort_values(["ticker", "type", "date"])
        trades_res.loc[trades_res.duplicated(subset="ticker"), "ticker"] = ""
        income_rub_sum = round(trades_res.amount_rub.sum(), 2)
        income_rest_sum = round(trades_res.amount_rub.sum() * 0.13, 2)
    else:
        print("Нет данных по сделкам")
        trades_res = None
        income_rub_sum = 0
        income_rest_sum = 0

    if interests is not None:
        interest_res = calculate_interest(interests)
        interest_rub_sum = interest_res.amount_rub.sum().round(2)
        interest_rest_sum = interest_res.rest.sum().round(2)
    else:
        print("Нет данных по начисленным на наличные процентам")
        interest_res = None
        interest_rub_sum = None
        interest_rest_sum = None

    print("Формирование отчета...")
    doc = DocxTemplate("template.docx")
    context = {
        "start_date": START_DATE,
        "year": year,
        "tbl_div": div_res.to_dict(orient="records") if div_res is not None else {},
        "div_sum": div_sum,
        "div_tax_paid_rub_sum": div_tax_paid_rub_sum,
        "div_tax_full_rub_sum": div_tax_full_rub_sum,
        "div_tax_rest_sum": div_tax_rest_sum,
        "tbl_div_accruals": div_accruals_res.to_dict(orient="records") if div_accruals_res is not None else {},
        "div_accruals_sum": div_accruals_sum,
        "div_accruals_tax_paid_rub_sum": div_accruals_tax_paid_rub_sum,
        "div_accruals_tax_full_rub_sum": div_accruals_tax_full_rub_sum,
        "div_accruals_tax_rest_sum": div_accruals_tax_rest_sum,
        "div_final_tax_rest_sum": div_final_tax_rest_sum,
        "div_final_sum": div_final_sum,
        "div_tax_paid_final_sum": div_tax_paid_final_sum,
        "div_tax_need_pay_final_sum": div_tax_need_pay_final_sum,
        # "tbl_cashflow": cash_flow_res.to_dict(orient="records") if cash_flow_res is not None else {},
        "tbl_trades": trades_res.to_dict(orient="records") if trades_res is not None else {},
        "tbl_interest": interest_res.to_dict(orient="records") if interest_res is not None else {},
        "interest_rub_sum": interest_rub_sum,
        "interest_rest_sum": interest_rest_sum,
        "income_rub_sum": income_rub_sum,
        "income_rest_sum": income_rest_sum,
        "tbl_fees": fees_res.to_dict(orient="records") if fees_res is not None else {},
        "fees_rub_sum": fees_rub_sum,
    }
    doc.render(context)
    doc.save(f"Пояснительная записка {year}.docx")


if __name__ == "__main__":
    main()
