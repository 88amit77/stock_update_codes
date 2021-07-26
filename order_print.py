import requests
import json
import psycopg2
import dropbox
from datetime import datetime


def get_approved_orders(account, x_seller_authz_token, order_id, fulfillment_model):
    fulfillment_model1=fulfillment_model.replace('_MODEL', '')
    url = "https://apigateway.snapdeal.com/seller-api/orders/print"

    payload = json.dumps({
        "fulfillmentType": fulfillment_model1,
        "orderCodes": [
            order_id
        ]
    })
    print("payload==",payload)


    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': x_seller_authz_token,
        'clientId': account['client_id'],
        'Content-Type': 'application/json',
        # 'Cookie': 'SCOUTER=z6ug035gf468ou; Megatron=!78M/eOIvaz5rKnT+ZidaGBcQKxXOrFqWpQL1VuoNxC9NUT0xqT5+50hNFZkZQdBkpfHVLoRGlbz/FQ=='
    }
    print("headers====",headers)

    # response = requests.request("POST", url, headers=headers, data=payload)
    # print("normal_response",response)
    # print("text_response",response.text)
    # res=response.text
    # json_data = json.loads(res)
    # print("json_data===",json_data)

    response = dict(requests.post(url, data=payload, headers=headers).json())
    print("response===..",response)
    # print("payload",response["payload"])
    payload=response["payload"]
    print("payload1",payload)
    if "downloadLink" in payload and "orderDetails" in payload:
        status='True'
    else:
        status='False'
    print(status)

    if status == 'True' :
        return {
            'status': True,
            'data': response,
            'error': ''
        }
    else:
        return {
        'status': False,
        'data': '',
        'error': response
    }



def get_snapdeal_accounts(cur_products):
    print('fetch accounts')
    accounts = []
    cur_products.execute("Select * from master_portalaccount where portal_name = 'snapdealp'")
    portal = cur_products.fetchone()
    accounts = []
    if portal is not None:
        portal_id = portal[0]
        cur_products.execute("Select * from master_portalaccountdetails where portal_id_id = 8 and account_id =10")
        portal_accounts = cur_products.fetchall()
        print("portal_accounts==>",portal_accounts)
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
    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)
    accounts = get_snapdeal_accounts(cur_products)
    error_log = ''
    for account in accounts:
        for warehouse in account['warehouses']:
            # cur_orders.execute(
            #     "Select no.dd_id, no.order_id, no.order_item_id, dd.fulfillment_model from api_neworder no inner join api_dispatchdetails dd on no.dd_id = dd.dd_id_id where no.portal_id = 8 and dd.is_mark_placed = False and no.order_id='SLP3220009792' and warehouse_id = " + str(
            #         warehouse['warehouse_id']))
            # orders = cur_orders.fetchall()
            # orders ={}
            # print("orders==",orders)
            # if len(orders):
            #     for order in orders:
            dd_id = '0009792'
            order_id = 'SLP3220939989'
            order_item_id = '3220009792'
            fulfillment_model = "DROPSHIP"
            approved_orders = get_approved_orders(account, warehouse['xsellerauthztoken'], order_id,
                                                  fulfillment_model)
            print("approved_orders==2>>",approved_orders)
            if approved_orders['status'] == True:
                payload = approved_orders['data']['payload']
                print("payload=2=",payload)
                download_link = payload['downloadLink']
                order_details = payload['orderDetails']
                for key in order_details:
                    if order_details[key]['operationSuccessful'] == True:
                        if key == order_id:
                            file = requests.get(download_link)
                            filename = order_id + '#' + order_item_id + '.pdf'
                            file_to = '/buymore2/orders/invoices/' + filename
                            dbx.files_upload(file.content, file_to, mode=dropbox.files.WriteMode.overwrite)
                            # cur_orders.execute(
                            #     'Update api_dispatchdetails set is_mark_placed = True where dd_id_id = ' + str(
                            #         dd_id))
                            # conn_orders.commit()
                        else:
                            error_log += 'order id ' + order_id + ' and dd_id ' + dd_id + ' not belong to same row\n'
                    else:
                        error_log += 'operation failed: ' + 'order id ' + order_id + ': ' + order_details[key][
                            'errorMessage'] + '\n'
            else:
                if 'metadata' in approved_orders['data']:
                    error_log += 'order id ' + order_id + ': ' + approved_orders['data']['metadata'][
                        'message'] + '\n'
                else:
                    error_log += 'order id ' + order_id + ': ' + 'Error 402\n'
    print('error_log',error_log)
    # error_log1="test"
    dbx.files_upload(error_log.encode(), '/buymore2/orders/invoices/logs/snapdeal_order_print_error_log' + str(
        int(datetime.timestamp(datetime.now()))) + '.log', mode=dropbox.files.WriteMode.overwrite)
    conn_orders.close()
    conn_products.close()
    return {"status": True}


print(lambda_handler())