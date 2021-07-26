import requests
import json

url = "https://apigateway.snapdeal.com/seller-api/products/inventory"

payload = json.dumps([
  {
    "supc": "SDL539465702",
    "availableInventory": "12"
  }
])
print("payload---",payload)
headers = {
  'x-auth-token': '300db07f-85f7-33dd-936e-614801f89bee',
  'X-Seller-AuthZ-Token': '9381cf6a-b03e-4ba9-a098-3584e124439b',
  'clientId': '197',
  'Content-Type': 'application/json',
  'Cookie': 'SCOUTER=zfodr71hiquul; Megatron=!3ul9rmhbaiX8wkT+ZidaGBcQKxXOrJXFrWHtBG70lUYYoJmQwjAEKFHD5iHFitaWLiWobExbxCv/gA=='
}


response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)