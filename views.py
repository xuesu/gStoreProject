from django.http import HttpResponse
from django.shortcuts import render

import gstore_connector

genre_list = gstore_connector.GstoreConnector.get_genres()


def main_view(request):
    result_list = gstore_connector.GstoreConnector.get_subjects(
        request.POST.get('name', '').strip(), request.POST.get('res_type', '').strip(), request.POST.get('genre', '').strip())
    return render(request, 'index.html', {'genre_list': [''] + genre_list,
                                          'result_list': result_list})


def detail_view(request):
    uri = request.GET.get('uri', '')
    detail_str_list = []
    if uri:
        try:
            detail_list = gstore_connector.GstoreConnector.get_predicates_objects(uri)
            for ele in detail_list:
                pred = ele[0]
                objc_list = ele[1]
                if pred.startswith(u'http://'):
                    pred = pred[pred.rfind(u'/') + 1:]
                for i in range(len(objc_list)):
                    if objc_list[i].startswith(u'http://'):
                        objc_list[i] = u'<a href="detail?uri=<{}>">{}</a>'.format(objc_list[i],
                                                                                  objc_list[i][
                                                                                  objc_list[i].rfind('/') + 1:])
                detail_str_list.append(u'<tr><td>{}</td>\n<td>{}</td>\n</tr>\n'.format(pred, '<br/>'.join(objc_list)))
        except AssertionError as e:
            pass
    return HttpResponse(u'''
    <!DOCTYPE html>
    <html lang="en">
        <body>
            <table>
                {}
            </table>
        </body>
    </html>
    '''.format('\n'.join(detail_str_list)) if detail_str_list else u'The list is Empty')


def blank_view(request):
    return HttpResponse("")
