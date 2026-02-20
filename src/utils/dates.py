from datetime import date, datetime, timedelta

def today_str():
    return date.today().strftime("%Y-%m-%d")

def get_vencimento_status(validade: date):
    if not validade:
        return "OK"
    days = (validade - date.today()).days
    if days < 0:
        return "VENCIDO"
    elif days <= 30:
        return "VENCENDO"
    return "OK"
