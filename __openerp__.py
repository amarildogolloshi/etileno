# -*- coding: utf-8 -*-
{
    'name': "ETiLeno",

    'summary': """
        ETiLeno is another extract, transform and load tool
        for Odoo.""",

    'description': """
        ETiLeno is another extract, transform and load tool
        for Odoo.
    """,

    'author': "Impulzia",
    'website': "http://www.impulzia.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/master/openerp/addons/base/module/module_data.xml
    # for the full list
    'category': 'Tools',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base'],

    # always loaded
    'data': [
        # 'security/ir.model.access.csv',
        #'templates.xml',
        'views/etileno.xml'
    ],
    # only loaded in demonstration mode
    #'demo': [
    #    'demo.xml',
    #],
}
