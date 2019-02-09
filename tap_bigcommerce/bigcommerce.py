import requests
import urllib



class Bigcommerce():

    base_url = "https://api.bigcommerce.com/stores/"

    endpoints = {
        'orders': {
            'version': 2,
            'url': 'orders'
        },
        'customers': {
            'version': 2,
            'url': 'customers'
        },
        'products': {
            'version': 3,
            'url': 'catalog/products'
        },
        'coupons': {
            'version': 2,
            'url': 'coupons'
        }
    }

    def __init__(self, client_id, access_token, store_hash):
        self.client_id = client_id
        self.access_token = access_token
        self.store_hash

        self.headers = {
            'accept': "application/json",
            'content-type': "application/json",
            'x-auth-client': self.client_id,
            'x-auth-token': self.access_token
        }

        self.base_url = self.base_url + self.store_hash + '/v{version}/'

    def make_url(self, version=2):

        def build(*res):
            url = self.base_url
            for r in res:
                url = '{}/{}'.format(url, r)
            return url

        return build

    def paginate():
        pass

    def get(self, url, params):

        r = request.get(url, params=params, headers=self.headers)

        if r.status != 200:
            raise requests.exceptions.HTTPError("HTTP {}: {}".format(
                r.status. r.json()
            ))

        self.last_response_headers = r.headers

        return r.json()

    



# def _handle_response(self, url, res, suppress_empty=True):
#         """
#         Adds rate limiting information on to the response object
#         """
#         result = Connection._handle_response(self, url, res, suppress_empty)
#         if 'X-Rate-Limit-Time-Reset-Ms' in res.headers:
#             self.rate_limit = dict(ms_until_reset=int(res.headers['X-Rate-Limit-Time-Reset-Ms']),
#                                    window_size_ms=int(res.headers['X-Rate-Limit-Time-Window-Ms']),
#                                    requests_remaining=int(res.headers['X-Rate-Limit-Requests-Left']),
#                                    requests_quota=int(res.headers['X-Rate-Limit-Requests-Quota']))
#             if self.rate_limiting_management:
#                 if self.rate_limiting_management['min_requests_remaining'] >= self.rate_limit['requests_remaining']:
#                     if self.rate_limiting_management['wait']:
#                         sleep(ceil(float(self.rate_limit['ms_until_reset']) / 1000))
#                     if self.rate_limiting_management.get('callback_function'):
#                         callback = self.rate_limiting_management['callback_function']
#                         args_dict = self.rate_limiting_management.get('callback_args')
#                         if args_dict:
#                             callback(args_dict)
#                         else:
#                             callback()

#         return result