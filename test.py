from accountData import AccountData
cr001 = AccountData(
	username = "cr001",
	client = "cr",
	parameter_name = "cr_cr001",
	master = "okx_usd_swap",
	slave = "okx_usdt_swap",
	principal_currency = "BTC",
	strategy = "funding",
    deploy_id = "cr_cr001@dt_okex_cswap_okex_uswap_btc")

# cr001.get_all_deploys()
print(1)