# -*- coding: utf-8 -*-

import base64
from lxml import etree
from openerp import models, fields, api
import coredb

F = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

class etileno_source(models.Model):
    _name = 'etileno.source'

    @api.one
    def verify_connection(self):
        db = coredb.DB(self.source_type, data=self.data)
        conn = db.connect(self.host, self.port, self.database, self.username, self.password)
        if conn:
            self.state = 'verified'

    @api.one
    def introspection(self):
        db = coredb.DB(self.source_type, data=self.data)
        conn = db.connect(self.host, self.port, self.database, self.username, self.password)
        tables = db.show_tables()
        table = self.env['etileno.table']
        for k,v in tables.items():
            print v
            # get fields
            fields = []
            for i in v['fields']:
                fields.append((0,0, {
                    'name': i[0],
                    'field_type': i[1],
                    'pk': i[0] in v['pk'] and (v['pk'].index(i[0]) + 1)
                }))
            # create table and fields
            data = {
                'source_id': self.id,
                'name': k,
                'rows': v['count'],
                'field_ids': fields
            }
            table.create(data)


    @api.onchange('source_type')
    def _onchange_source_type_(self):
        # check if empty
        if self.source_type:
            self.port = coredb.modules[self.source_type]['port']
        else:
            self.port = None

    @api.model
    def create(self, vals):
        vals['state'] = 'draft' # add default state value here
        res = super(etileno_source, self).create(vals)
        return res


    # TODO: add tunel ssh info and connection string
    name = fields.Char(required=True)
    source_type = fields.Selection(coredb.engines, string='Source type', default=coredb.engines[0][0], required=True)
    filename = fields.Char()
    data = fields.Binary() # for CSV, etc.
    host = fields.Char(default='127.0.0.1', required=True)
    port = fields.Integer()
    database = fields.Char()
    username = fields.Char()
    password = fields.Char()
    table_ids = fields.One2many('etileno.table', 'source_id')
    state = fields.Selection([
        ('draft', 'Draft'),
        ('verified', 'Verified'),
        ('instrospection', 'Introspection'),
        ('sync', 'Sync'),
        ('done', 'Done')
    ])


class etileno_table(models.Model):
    _name = 'etileno.table'

    @api.multi
    def action_name (self):
        view = self.env.ref('etileno.view_etileno_table_form').id
        return {
            'type': 'ir.actions.act_windows',
            'res_id': self.id
        }

    @api.multi
    def reload_page(self):
        return {
            'type': 'ir.actions.client',
            'tag': 'reload',  }

        model_obj = self.env['ir.model.data']
        data_id = model_obj._get_id('etileno', 'view_etileno_table_form')
        view_id = model_obj.browse(data_id).res_id
        return {
            'type': 'ir.actions.act_window',
            'name': 'String',
            'res_model': 'model.name',
            'view_type' : 'form',
            'view_mode' : 'form',
            'view_id' : view_id,
            'target' : 'current',
            'nodestroy' : True,
        }

    @api.model
    def fields_view_get(self, view_id=None, view_type='tree', context=None, toolbar=False, submenu=False):
        res = super(etileno_table, self).fields_view_get(view_id=view_id, view_type=view_type, context=self.env.context, toolbar=toolbar, submenu=submenu)
        print '>>>', self.env.context
        if self.env.context.has_key('params'):
            params = self.env.context['params']
            if view_type == 'form' and params.get('view_type', None) == 'form' and params['model'] == 'etileno.table':
                id = self.env.context['params']['id']
                # check if fields < visible columns to show
                if self.env['etileno.field'].search_count([('table_id', '=', id)]) <= 10:
                    fields = [i.name for i in self.env['etileno.field'].search([('table_id', '=', id)])]
                else:
                    fields = [i.name for i in self.env['etileno.field'].search([('table_id', '=', id), ('visible', '=', True)])]
                if fields:
                    print fields
                    t = res['fields']['row_ids']['views']['tree']['arch']
                    for i in xrange(10):
                        if i < len(fields):
                            t = t.replace('#%s#' % F[i], fields[i])
                        else:
                            t = t.replace('#%s#' % F[i], '')
                    res['fields']['row_ids']['views']['tree']['arch'] = t
        return res

    @api.one
    def _get_rows_related(self):
        if self.env['etileno.field'].search_count([('table_id', '=', self.id)]) <= 10:
            fields = [i.name for i in self.env['etileno.field'].search([('table_id', '=', self.id)])]
        else:
            fields = [i.name for i in self.env['etileno.field'].search([('table_id', '=', self.id), ('visible', '=', True)])]
        if fields:
            # TODO: to keep simple connections
            db = coredb.DB(self.source_id.source_type)
            conn = db.connect(self.source_id.host, self.source_id.port, self.source_id.database, self.source_id.username, self.source_id.password)
            rows = db.get_data(self.name, fields)

            # add column name
            #data = dict([(F[i], fields[i]) for i in xrange(len(fields))])
            #self.row_ids |= self.env['etileno.row'].create(data)

            # add data rows
            for row in rows:
                data = {}
                for i in xrange(len(fields)):
                    data[F[i]] = row[fields[i]]
                self.row_ids |= self.env['etileno.row'].create(data)

    source_id = fields.Many2one('etileno.source', 'Source')
    field_ids = fields.One2many('etileno.field', 'table_id', 'Fields')
    source_type = fields.Selection(related='source_id.source_type', string='DB Type', store=True, readonly=True)
    name = fields.Char()
    rows = fields.Integer()
    row_ids = fields.One2many('etileno.row', 'table_id', string='Rows', compute='_get_rows_related')
    model = fields.Many2one('ir.model') # default model to map
    info = fields.Text()


class etileno_field(models.Model):
    _name = 'etileno.field'

    #@api.model
    #def fields_view_get(self, view_id=None, view_type='tree', context=None, toolbar=False, submenu=False):
    #    res = super(etileno_field,self).fields_view_get(view_id=view_id, view_type=view_type, toolbar=toolbar, submenu=submenu)
    #    return res

    table_id = fields.Many2one('etileno.table', 'Table', readonly=True)
    source = fields.Many2one(related='table_id.source_id', store=True, readonly=True)
    name = fields.Char()
    field_type = fields.Char('Type')
    pk = fields.Integer('PK') # primary key order (1, 2, 3...)
    visible = fields.Boolean()
    fk_id = fields.Many2one('etileno.field')
    fk_child_ids = fields.One2many('etileno.field', 'fk_id')


class etileno_map(models.Model):
    """map between external fields and odoo fields"""
    _name = 'etileno.map'

    #table_id = fields.Many2one('etileno.table')
    field_id = fields.Many2one('etileno.field')
    odoo_field_id = fields.Many2one('ir.models.field')
    action_id = fields.Many2one('etileno.action')
    constraint = fields.Boolean() # check value before create
    transform = fields.Text() # python code to eval

    def write(self):
        pass


class etileno_task_action(models.Model):
    # TODO: basic constaint - it need its own model
    # TODO: check for primary keys to create data
    _name = 'etileno.task.action'

    order = fields.Integer()
    task_id = fields.Many2one('etileno.task')
    source = fields.Many2one(related='field_id.source', readonly=True)
    table = fields.Many2one(related='field_id.table_id', readonly=True)
    field_id = fields.Many2one('etileno.field')
    odoo_model = fields.Many2one(related='odoo_field_id.model_id', readonly=True)
    odoo_field_id = fields.Many2one('ir.model.fields')
    action = fields.Selection([
        ('c', 'copy'),
        ('sr', 'search & replace'),
        ('r', 'replace'),
    ], default='c')
    transform = fields.Text() # python code to eval (alpha)


class etileno_task(models.Model):
    _name = 'etileno.task'

    @api.multi
    def run_task(self):
        # TODO: add log / pk
        source = {}
        #data = {}
        fields = []

        # group fields by table
        for i in self.task_action_ids:
            if not source.has_key(i.source):
                source[i.source] = []
            source[i.source].append(i)

        for source, actions in source.items():
            if source.source_type == 'csv':
                db = coredb.DB(source.source_type, data=source.data)
                conn = db.connect()
                rows = db.get_rows()
                for row in rows.dicts():
                    # d = {model: {data fields}}
                    d = {}
                    for action in actions:
                        if not d.has_key(action.odoo_model.model):
                            d[action.odoo_model.model] = {}
                        d[action.odoo_model.model][action.odoo_field_id.name] = row[action.field_id.name]

                    for model, data in d.items():
                        self.env[model].create(data)
            elif source.source_type == 'pymssql':
                # get tables and fields
                tables = {}
                for action in actions:
                    if not tables.has_key(action.table):
                        tables[action.table.name] = []
                    tables[action.table.name].append(action.field_id.name)

                # connect with database
                db = coredb.DB(source.source_type)
                conn = db.connect(source.host, source.port, source.database, source.username, source.password)
                if not conn:
                    print 'ERROR'

                # get rows from tables
                for table, fields in tables.items():
                    rows = db.get_rows(table=table, fields=fields)

                    for row in rows:
                        d = {}
                        for action in actions:
                            if not d.has_key(action.odoo_model.model):
                                d[action.odoo_model.model] = {}
                            d[action.odoo_model.model][action.odoo_field_id.name] = row[action.field_id.name]

                        for model, data in d.items():
                            print model, data
                            self.env[model].create(data)



    name = fields.Char()
    task_action_ids = fields.One2many('etileno.task.action', 'task_id')
    constraint = fields.Char() # eval this to perform an action... (alpha)


class etileno_row(models.TransientModel):
    """Fake model to show data from sources"""
    _name = 'etileno.row'

    table_id = fields.Many2one('etileno.table')
    a = fields.Char() # 1
    b = fields.Char() # 2
    c = fields.Char() # 3
    d = fields.Char() # 4
    e = fields.Char() # 5
    f = fields.Char() # 6
    g = fields.Char() # 7
    h = fields.Char() # 8
    i = fields.Char() # 9
    j = fields.Char() # 10


class etileno_log(models.Model):
    _name = 'etileno.log'

    name = fields.Char()
    level = fields.Selection([
        ('info', 'Info'),
        ('warning', 'Warning'),
        ('error', 'Error'),
        ('debug', 'Debug')
    ], default='info')
    time = fields.Datetime()
    message = fields.Text()
