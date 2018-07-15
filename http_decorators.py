from urllib.parse import urlparse, parse_qs

from utils import can_be_converted_to_int, is_http_url


def check_params_and_path(func):
    def wrapper(self, *args, **kwargs):
        parsed_url = urlparse(self.path)
        message_template = '<b>{}</b>'
        content_type = 'text/html'
        if parsed_url.path != '/':
            self.response(status_code=404, data=message_template.format('Not found'), content_type=content_type)
        else:
            parsed_query_params = parse_qs(parsed_url.query)
            if func.__name__ == "do_GET":
                job_id = parsed_query_params.get('get')
                if job_id and len(job_id) == 1 and can_be_converted_to_int(job_id[0]):
                    return func(self, *args, **kwargs)
                else:
                    message = message_template.format('Bad Request. Please, check get|int parameter.')
                    self.response(status_code=400, data=message, content_type=content_type)
            elif func.__name__ == "do_POST":
                concurrency = parsed_query_params.get('concurrency')
                url = parsed_query_params.get('url')
                if (concurrency and url) and (len(concurrency) == 1 and len(url) == 1) and \
                        can_be_converted_to_int(concurrency[0]) and is_http_url(url[0]):
                    return func(self, *args, **kwargs)
                else:
                    message = message_template.format('Bad Request. Please, check concurrency|int and url parameter.')
                    self.response(status_code=400, data=message, content_type=content_type)

    return wrapper
