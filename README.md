# kite-bookmap-adapter
This is an adapter for using bookmap to connect NSE exchange using the zerodha websocket.

Run Maven Build. Select the jar with dependencies from the target folder and place it in C:\Bookmap\API\Layer0ApiModules.

 For login:
 - Username is your API Key obtained from Zerodha.
- Password is the accessToken which is generated using the request token from the Postback URL.

There is no way of automating the accessToken request as of now due to NSE regulation to login once everyday manually.