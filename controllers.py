# -*- coding: utf-8 -*-
from openerp import http

# class Etileno(http.Controller):
#     @http.route('/etileno/etileno/', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/etileno/etileno/objects/', auth='public')
#     def list(self, **kw):
#         return http.request.render('etileno.listing', {
#             'root': '/etileno/etileno',
#             'objects': http.request.env['etileno.etileno'].search([]),
#         })

#     @http.route('/etileno/etileno/objects/<model("etileno.etileno"):obj>/', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('etileno.object', {
#             'object': obj
#         })
