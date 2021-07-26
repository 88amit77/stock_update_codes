import requests
import json
import psycopg2
import dropbox
from datetime import datetime


def get_reprint_orders(account, x_seller_authz_token, order_id):
    url = 'http://apigateway.snapdeal.com/seller-api/orders/reprint'
    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': x_seller_authz_token,
        'clientId': account['client_id'],
        'Content-Type': 'application/json',
           }
    payload = json.dumps({
        "orderCodes": [
            order_id
        ]
    })

    response = dict(requests.post(url, data=payload, headers=headers).json())
    print("response===--", response)

    if 'payload' not in response:
        return {
            'status': False,
            'data': '',
            'error': response
        }
    else:
        return {
            'status': True,
            'data': response,
            'error': ''
        }


def get_snapdeal_accounts(cur_products):
    print('fetch accounts')
    accounts = []
    cur_products.execute("Select * from master_portalaccount where portal_name = 'snapdealp'")
    portal = cur_products.fetchone()
    accounts = []
    if portal is not None:
        portal_id = portal[0]
        cur_products.execute("Select * from master_portalaccountdetails where portal_id_id = " + str(portal_id))
        portal_accounts = cur_products.fetchall()
        if len(portal_accounts):
            for portal_account in portal_accounts:
                xauthtoken = ''
                xsellerauthztoken = ''
                client_id = ''
                access_token_field = portal_account[4]
                for item in access_token_field:
                    if item['name'] == 'xauthtoken':
                        xauthtoken = item['base_url']
                    elif item['name'] == 'xsellerauthztoken':
                        xsellerauthztoken = item['base_url']
                    elif item['name'] == 'clientid':
                        client_id = item['base_url']
                warehouses = []
                warehouse_field = portal_account[5]
                for item in warehouse_field:
                    warehouse = {
                        'warehouse_id': item['warehouse_id'],
                        'warehouse_code': item['portal_warehouse'],
                        'xsellerauthztoken': xsellerauthztoken
                    }
                    warehouses.append(warehouse)
                account = {
                    "portal_name": portal[1],
                    "account_name": portal_account[1],
                    "portal_id": portal_id,
                    "account_id": portal_account[0],
                    "xauthtoken": xauthtoken,
                    "client_id": client_id,
                    "warehouses": warehouses
                }
                accounts.append(account)
    return accounts


# def lambda_handler(event, context):
def lambda_handler():
    conn_orders = psycopg2.connect(database="orders", user="postgres", password="buymore2",
                                   host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    conn_products = psycopg2.connect(database="products", user="postgres", password="buymore2",
                                     host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")

    cur_orders = conn_orders.cursor()
    cur_products = conn_products.cursor()
    access_token = 'd7ElXR2Sr-AAAAAAAAAAC2HC0qc45ss1TYhRYB4Jy6__NJU1jjGiffP7LlP_2rrf'
    dbx = dropbox.Dropbox(access_token)
    accounts = get_snapdeal_accounts(cur_products)
    error_log = ''
    for account in accounts:
        for warehouse in account['warehouses']:
            cur_orders.execute(
                'Select no.dd_id, no.order_id, no.order_item_id from api_neworder no inner join api_dispatchdetails dd on no.dd_id = dd.dd_id_id where no.portal_id = 8 and dd.is_mark_placed = True and dd.packing_status = True and warehouse_id = ' + str(
                    warehouse['warehouse_id']))
            orders = cur_orders.fetchall()
            if len(orders):
                for order in orders:
                    dd_id = order[0]
                    order_id = order[1]
                    order_item_id = order[2]
                    approved_orders = get_reprint_orders(account, warehouse['xsellerauthztoken'], order_id)
                    if approved_orders['status'] == True:
                        payload = approved_orders['data']['payload']
                        if payload['operationSuccessful'] == True:
                            download_link = payload['downloadLink']

                            file = requests.get(download_link)
                            filename = order_id + '#' + order_item_id + '.pdf'
                            file_to = '/buymore2/orders/invoices/' + filename
                            dbx.files_upload(file.content, file_to, mode=dropbox.files.WriteMode.overwrite)
                            cur_orders.execute(
                                "Update api_dispatchdetails set have_invoice_file = True, is_dispatch = True, status = 'dispatched' where dd_id_id = " + str(
                                    dd_id))
                            conn_orders.commit()
                        else:
                            error_log += 'operation failed: ' + 'order id ' + order_id + ': ' + payload[
                                'errorMessage'] + '\n'
                    else:
                        if 'metadata' in approved_orders['data']:
                            error_log += 'order id ' + order_id + ': ' + approved_orders['data']['metadata'][
                                'message'] + '\n'
                        else:
                            error_log += 'order id ' + order_id + ': ' + 'Error 402\n'
    dbx.files_upload(error_log.encode(), '/buymore2/orders/invoices/logs/snapdeal_order_reprint_error_log' + int(
        datetime.now().timestamp()) + '.log', mode=dropbox.files.WriteMode.overwrite)
    conn_orders.close()
    conn_products.close()
    return {"status": True}


print(lambda_handler())