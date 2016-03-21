# from ipdb import set_trace as i_trace
# i_trace()

class pgSQL_Functions:
    """

    NOTE: USE plpythonu and plluau for WRITE ACCESS

    """

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    def exists(self,funct_name):
        qry                                 =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_proc
                                                    WHERE proname='%s');
                                                """ % funct_name
        return                                  self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Check:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def primary_key(self,table_name):
            qry                             =   """
                                                select relhasindex has_index
                                                from pg_class
                                                where relnamespace=2200
                                                and relkind='r'
                                                and relname=quote_ident('%s');
                                                """ % table_name
            x                               =   self.T.pd.read_sql(qry,self.T.eng)
            return                              True if len(x['has_index']) and x['has_index'][0]==True else False

    class Run:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def make_column_primary_serial_key(self,table_name,uid_col='uid'):
            """
            Usage: make_column_primary_serial_key('table_name','uid_col',is_new_col=True)
            """
            _info                           =   self.F.tables_get_info(table_name)
            is_new_col                      =   False if _info.iloc[:,0].astype(str).tolist().count(uid_col) else True
            if is_new_col:
                is_primary_key,has_default  =   False,False
            else:
                is_primary_key              =   True if _info[_info.column_name==uid_col].ix[:,'is_primary_key'].tolist()[0] else False
                has_default                 =   True if _info[_info.column_name==uid_col].ix[:,'column_default'].tolist()[0] else False
                

            T                               =   {'tbl'                  :   table_name,
                                                 'uid_col'              :   uid_col,
                                                 'is_new_col'           :   is_new_col,
                                                 'is_primary_key'       :   is_primary_key,
                                                 'has_default'          :   has_default}
            cmd                             =   """select z_make_column_primary_serial_key( '%(tbl)s',
                                                                                        '%(uid_col)s',
                                                                                         %(is_new_col)s,
                                                                                         %(is_primary_key)s,
                                                                                         %(has_default)s);
                                                """ % T
            self.T.to_sql(                      cmd)

        def confirm_extensions(self,exts=['plpythonu','pllua','plsh'],verbose=False):
            qry =   '\n'.join(['CREATE EXTENSION IF NOT EXISTS %s;' % it for it in exts])
            self.T.to_sql(                      qry)
            if verbose:                         print 'Extensions Confirmed'
            return

        def reset_index(self,table_name,uid_col,sort_by=''):
            qry="""
                ALTER TABLE %(tbl)s ADD COLUMN new_%(uid_col)s INTEGER;
                WITH upd as (
                    SELECT %(uid_col)s,row_number() over() new_%(uid_col)s
                    FROM %(tbl)s
                    ORDER BY %(sort_by)s
                    )
                UPDATE %(tbl)s _s
                SET new_%(uid_col)s = upd.new_uid
                FROM upd
                WHERE upd.%(uid_col)s = _s.%(uid_col)s;
                ALTER TABLE %(tbl)s ALTER COLUMN %(uid_col)s TYPE INTEGER USING new_%(uid_col)s;
                ALTER TABLE %(tbl)s DROP COLUMN new_%(uid_col)s;

            """ % {'tbl':table_name,'uid_col':uid_col,'sort_by':sort_by}
            self.T.to_sql(qry)

    class Create:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def __show_auto_groups(self):
            (_out,_err) = self.T.sub_popen('ls -p %s/sql_functions/ | grep "/"' % self.T.pg_classes_pwd,
                   stdout=self.T.sub_PIPE,
                   shell=True).communicate()

            assert _err is None
            print _out

        def batch_groups(self,grps=['admin','json','strings']):
            cmds = []
            c_tmp = 'psql -d linkedin -c "%s ' + self.T.pg_classes_pwd + '/sql_functions/%s/%s;"'
            
            for d in grps:
                for f in sorted(self.T.os.listdir('%s/sql_functions/%s' % (self.T.pg_classes_pwd,d))):
                    if f.count('.sql'):
                        cmds.append( c_tmp % ('\\\\\\\\i',d,f) )
            cmds = '\n'.join(cmds)
            qry = """
                DO E'#!/bin/sh

                    export PGOPTIONS="--client-min-messages=warning"
                    %s
                    unset PGOPTIONS
                    ' LANGUAGE plsh;
                """ % cmds
            self.T.to_sql(                      qry)

class pgSQL_Triggers:

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    class Exists:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def event_trigger(self,trigger_name):
            qry                             =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_event_trigger
                                                    WHERE evtname='%s'
                                                    AND evtenabled='O');
                                                """ % trigger_name
            return                              self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Enabled:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def event_trigger(self,trigger_name):
            qry                             =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_event_trigger
                                                    WHERE evtname='%s');
                                                """ % trigger_name
            return                              self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Create:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def z_auto_add_primary_key(self):
            self.F.functions_create_z_next_free()
            c                           =   """
                DROP FUNCTION if exists z_auto_add_primary_key() CASCADE;

                CREATE OR REPLACE FUNCTION z_auto_add_primary_key()
                    RETURNS event_trigger AS
                $BODY$
                DECLARE
                    has_index boolean;
                    tbl_name text;
                    primary_key_col text;
                    missing_primary_key boolean;
                    has_uid_col boolean;
                    _seq text;
                BEGIN
                    select relhasindex,relname into has_index,tbl_name
                        from pg_class
                        where relnamespace=2200
                        and relkind='r'
                        order by oid desc limit 1;
                    IF (
                        pg_trigger_depth()=0
                        and has_index=False )
                    THEN
                        --RAISE NOTICE 'NOT HAVE INDEX';
                        EXECUTE format('SELECT a.attname
                                        FROM   pg_index i
                                        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                                             AND a.attnum = ANY(i.indkey)
                                        WHERE  i.indrelid = ''%s''::regclass
                                        AND    i.indisprimary',tbl_name)
                        INTO primary_key_col;

                        missing_primary_key = (select primary_key_col is null);

                        IF missing_primary_key=True
                        THEN
                            --RAISE NOTICE 'IS MISSING PRIMARY KEY';
                            _seq = format('%I_uid_seq',tbl_name);
                            EXECUTE format('select count(*)!=0
                                        from INFORMATION_SCHEMA.COLUMNS
                                        where table_name = ''%s''
                                        and column_name = ''uid''',tbl_name)
                            INTO has_uid_col;
                            IF (has_uid_col=True)
                            THEN
                                --RAISE NOTICE 'HAS UID COL';
                                execute format('alter table %I
                                                    alter column uid type integer,
                                                    alter column uid set not null,
                                                    alter column uid set default z_next_free(
                                                        ''%I''::text,
                                                        ''uid''::text,
                                                        ''%I''::text),
                                                    ADD PRIMARY KEY (uid)',tbl_name,tbl_name,'_seq');
                            ELSE
                                --RAISE NOTICE 'NOT HAVE UID COL';
                                _seq = format('%I_uid_seq',tbl_name);
                                execute format('alter table %I add column uid integer primary key
                                                default z_next_free(
                                                        ''%I''::text,
                                                        ''uid''::text,
                                                        ''%I''::text)',tbl_name,tbl_name,'_seq');
                            END IF;

                        END IF;

                    END IF;

                END;
                $BODY$
                    LANGUAGE plpgsql;

                DROP EVENT TRIGGER if exists missing_primary_key_trigger;
                CREATE EVENT TRIGGER missing_primary_key_trigger
                ON ddl_command_end
                WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
                EXECUTE PROCEDURE z_auto_add_primary_key();
                                                """
            self.T.to_sql(                      c)
            print 'Added: f(x) z_auto_add_primary_key'
        def z_auto_add_last_updated_field(self):
            c                           =   """
                DROP FUNCTION if exists z_auto_add_last_updated_field() cascade;

                CREATE OR REPLACE FUNCTION z_auto_add_last_updated_field()
                    RETURNS event_trigger AS
                $BODY$
                DECLARE
                    last_table TEXT;
                    has_last_updated BOOLEAN;
                BEGIN
                    last_table := ( SELECT relname FROM pg_class
                                    WHERE relnamespace=2200
                                    AND relkind='r'
                                    ORDER BY oid DESC LIMIT 1);

                    EXECUTE 'SELECT EXISTS ('
                        || ' SELECT 1'
                        || ' FROM information_schema.columns'
                        || ' WHERE table_name='''
                        || quote_ident(last_table)
                        || ''' AND column_name=''last_updated'''
                        || ' )'
                        INTO has_last_updated;

                    -- RAISE EXCEPTION 'has_last_updated is %', has_last_updated;


                    IF (
                        pg_trigger_depth()=0
                        AND has_last_updated=False
                        AND position('tmp_' in last_table)=0  --exclude public.tmp_*
                        )
                    THEN
                        EXECUTE FORMAT('ALTER TABLE %I DROP COLUMN IF EXISTS last_updated',last_table);
                        EXECUTE FORMAT('ALTER TABLE %I ADD COLUMN last_updated timestamp WITH TIME ZONE',last_table);
                        EXECUTE FORMAT('DROP FUNCTION IF EXISTS z_auto_update_timestamp_on_%s_in_last_updated() CASCADE',last_table);
                        EXECUTE FORMAT('DROP TRIGGER IF EXISTS update_timestamp_on_%s_in_last_updated ON %s',last_table,last_table);

                        EXECUTE FORMAT('CREATE OR REPLACE FUNCTION z_auto_update_timestamp_on_%s_in_last_updated()'
                                        || ' RETURNS TRIGGER AS $$'
                                        || ' BEGIN'
                                        || '     NEW.last_updated := now();'
                                        || '     RETURN NEW;'
                                        || ' END;'
                                        || ' $$ language ''plpgsql'';'
                                        || '',last_table);

                        EXECUTE FORMAT('CREATE TRIGGER update_timestamp_on_%s_in_last_updated'
                                        || ' BEFORE UPDATE OR INSERT ON %I'
                                        || ' FOR EACH ROW'
                                        || ' EXECUTE PROCEDURE z_auto_update_timestamp_on_%s_in_last_updated();'
                                        || '',last_table,last_table,last_table);

                    END IF;

                END;
                $BODY$
                    LANGUAGE plpgsql;

                DROP EVENT TRIGGER if exists missing_last_updated_field;
                CREATE EVENT TRIGGER missing_last_updated_field
                ON ddl_command_end
                WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
                EXECUTE PROCEDURE z_auto_add_last_updated_field();
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)
        def z_auto_update_timestamp(self,tbl,col):
            a="""
                DROP FUNCTION if exists z_auto_update_timestamp_on_%(tbl)s_in_%(col)s() cascade;
                DROP TRIGGER if exists update_timestamp_on_%(tbl)s_in_%(col)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_auto_update_timestamp_on_%(tbl)s_in_%(col)s()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.last_updated := now();
                    RETURN NEW;
                END;
                $$ language 'plpgsql';

                CREATE TRIGGER update_timestamp_on_%(tbl)s_in_%(col)s
                BEFORE UPDATE OR INSERT ON %(tbl)s
                FOR EACH ROW
                EXECUTE PROCEDURE z_auto_update_timestamp_on_%(tbl)s_in_%(col)s();

            """ % {'tbl':tbl,'col':col}

            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    a)
            return

    class Destroy:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def z_auto_add_primary_key(self):
            c                           =   """
            DROP FUNCTION if exists
                z_auto_add_primary_key() cascade;

            DROP EVENT TRIGGER if exists missing_primary_key_trigger cascade;
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)
        def z_auto_add_last_updated_field(self):
            c                               =   """
            DROP FUNCTION if exists
                z_auto_add_last_updated_field() cascade;

            DROP EVENT TRIGGER if exists missing_last_updated_field;
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)

    class Operate:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def disable_tbl_trigger(self,tbl,trigger_name):
            cmd = "ALTER TABLE %(tbl)s DISABLE TRIGGER %(trig)s;" % {'tbl':tbl,'trig':trigger_name}
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
        def enable_tbl_trigger(self,tbl,trigger_name):
            if trigger_name=='z_auto_add_primary_key':
                trigger_name                =   'missing_primary_key_trigger'
            cmd = "ALTER TABLE %(tbl)s ENABLE TRIGGER %(trig)s;" % {'tbl':tbl,'trig':trigger_name}
            self.T.to_sql(                      cmd)
        def disable_event_trigger(self,trigger_name):
            if trigger_name=='z_auto_add_primary_key':
                trigger_name                =   'missing_primary_key_trigger'
            cmd                             =   'ALTER EVENT TRIGGER %s DISABLE' % trigger_name
            self.T.to_sql(                      cmd)
        def enable_event_trigger(self,trigger_name):
            cmd                             =   'ALTER EVENT TRIGGER %s ENABLE' % trigger_name
            self.T.to_sql(                      cmd)

class pgSQL_Tables:

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    def exists(self,table_name):
        qry                                 =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM information_schema.tables
                                                    WHERE table_schema='public'
                                                    AND table_name='%s');
                                                """ % table_name
        return                                  self.T.pd.read_sql(qry,self.T.eng).exists[0]

    def get_info(self,table_name):
        qry                                 =   """
                                                WITH primary_key_info AS (
                                                    SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type_pk
                                                    FROM   pg_index i
                                                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                                                         AND a.attnum = ANY(i.indkey)
                                                    WHERE  i.indrelid = '%s'::regclass
                                                    AND    i.indisprimary
                                                    ),
                                                gen_info AS (
                                                    SELECT 
                                                        column_name, data_type, character_maximum_length,
                                                        column_default,is_nullable,data_type_pk
                                                    FROM INFORMATION_SCHEMA.COLUMNS
                                                    LEFT JOIN primary_key_info pk ON pk.attname = column_name
                                                    WHERE table_name = '%s'
                                                    )    
                                                SELECT 
                                                    column_name, data_type, character_maximum_length,
                                                    column_default,
                                                        CASE
                                                        WHEN data_type_pk is NULL THEN NULL
                                                        ELSE true END is_primary_key,
                                                    is_nullable
                                                FROM gen_info;

                                                """ % (table_name,table_name)
        return                                  self.T.pd.read_sql(qry,self.T.eng)

    def has_col(self,table_name,column_name):
        qry                                 =   """
                                                SELECT EXISTS (
                                                    SELECT 1
                                                    FROM INFORMATION_SCHEMA.COLUMNS
                                                    WHERE table_name = '%s'
                                                    AND column_name = '%s'
                                                    );
                                                """ % (table_name,column_name)
        return                                  self.T.pd.read_sql(qry,self.T.eng).exists[0]

    def get_tables_and_indicies(self):
        qry                                 =   """
                                                SELECT pg_class.relname, pg_attribute.attname
                                                    FROM pg_class, pg_attribute, pg_index
                                                    WHERE pg_class.oid = pg_attribute.attrelid AND
                                                        pg_class.oid = pg_index.indrelid AND
                                                        pg_index.indkey[0] = pg_attribute.attnum AND
                                                        pg_index.indisprimary = 't';
                                                """
        return                                  self.T.pd.read_sql(qry,self.T.eng)

    class Update:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

    class Create:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def new(self,tbl_name,tbl_dict,drop_if_exists=False):
            tbl_cols = ',\n'.join([ '%s %s' % (k,v) for k,v in tbl_dict.iteritems()])
            qry = "CREATE TABLE IF NOT EXISTS %s ( %s );" % (tbl_name,tbl_cols)
            if drop_if_exists:
                qry = "DROP TABLE IF EXISTS %s CASCADE; " % tbl_name + qry
            self.T.to_sql(                  qry)
            self.F.functions_run_make_column_primary_serial_key(tbl_name)
            # if not self.F.functions_check_primary_key(tbl_name):
            #     self.F.functions_run_make_column_primary_serial_key(tbl_name,'uid')

class pgSQL_Types:

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    def exists(self,type_name):
        qry                                 =   """ SELECT EXISTS (SELECT 1
                                                    FROM pg_type
                                                    WHERE typname = '%s')
                                                """ % type_name
        return                                  self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Create:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def string_dist_results(self):
            qry="""
                DROP TYPE IF EXISTS string_dist_results cascade;
                CREATE TYPE string_dist_results as (
                    idx integer,
                    orig_str text,
                    jaro double precision,
                    jaro_b text,
                    leven integer,
                    leven_b text,
                    nysiis text,
                    rating_codex text
                );
            """
            self.T.to_sql(                  qry)

    class Destroy:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def string_dist_results(self):
            qry="""DROP TYPE IF EXISTS string_dist_results cascade;
            """
            self.T.to_sql(                  qry)

class pgSQL:
    

    def __init__(self,**kwargs):
        """

            pgSQL(db_settings=[DB_NAME, DB_USER, DB_PW, DB_HOST, DB_PORT])

        """

        def download_file(url,save_path):
            import os
            _dir = save_path[:save_path.rfind('/')]
            if not os.path.exists(_dir):
                os.makedirs(_dir)

            with open(save_path, 'wb') as handle:
                response = self.T.requests.get( url, stream=True)

                if not response.ok:
                    # Something went wrong
                    print 'error'

                for block in response.iter_content(1024):
                    if not block:
                        break

                    handle.write(block)
                    handle.flush()
            return True

        def read_json_from_url_response(url):
            r = self.T.requests.get(url)
            assert r.status_code=='200'
            # print r.text
            g = r.text
            g = g.replace('true',"'true'")
            a = eval(g)
            return a

        def to_sql(cmd):
            self.T.conn.set_isolation_level(    0)
            self.T.cur.execute(                 cmd)

        def redirect_logs_to_file(file_desc='/dev/pts/0',msg_form="%(asctime)s - %(levelname)s - %(message)s"):
            # print T.logger.__dict__
            # print T.logger.manager.__dict__

            # for it in dir(logger):
            #     print it,getattr(logger,it)

            for it in self.T.logger.handlers:
                self.T.logger.removeHandler(it)

            for it in self.T.logger.parent.handlers:
                self.T.logger.parent.removeHandler(it)

            for it in self.T.logger.root.handlers:
                self.T.logger.root.removeHandler(it)

            # print logger.manager.__dict__
            del_these                       =   ['IPKernelApp','basic_logger']
            for it in del_these:
                if self.T.logger.manager.__dict__['loggerDict'].has_key(it):
                    del self.T.logger.manager.__dict__['loggerDict'][it]

            for k in self.T.logger.manager.__dict__['loggerDict'].keys():
                if k.count('sqlalchemy') or k.count('pandas'):
                    del self.T.logger.manager.__dict__['loggerDict'][k]

            self.T.logging.basicConfig(filename=file_desc, level=self.T.logging.DEBUG, format=msg_form)
            return

        def custom_geoseries_plot(s,figsize=(8,8)):
            # s=T.gd.GeoSeries(A)
            colormap='Set1'
            axes=None
            linewidth=1.0

            import matplotlib.pyplot as plt
            if axes is None:
                fig, ax = plt.subplots(figsize=figsize)
                ax.set_aspect('equal')
            else:
                ax = axes
            ax.get_xaxis().get_major_formatter().set_scientific(False)
            ax.get_xaxis().get_major_formatter().set_useOffset(False)
            plt.xticks(rotation='vertical')
            ax.get_yaxis().get_major_formatter().set_scientific(False)
            ax.get_yaxis().get_major_formatter().set_useOffset(False)
            color = T.gd.plotting.gencolor(len(s), colormap=colormap)
            for geom in s:
                if geom.type == 'Polygon' or geom.type == 'MultiPolygon':
                    T.gd.plotting.plot_multipolygon(ax, geom, facecolor=next(color), linewidth=linewidth)
                elif geom.type == 'LineString' or geom.type == 'MultiLineString':
                    T.gd.plotting.plot_multilinestring(ax, geom, color=next(color), linewidth=linewidth)
                elif geom.type == 'Point':
                    T.gd.plotting.plot_point(ax, geom)
            plt.ticklabel_format(style='plain')
            plt.grid()
            plt.draw()

        import                                  datetime                as dt
        from dateutil                           import parser           as DU               # e.g., DU.parse('some date as str') --> obj(datetime.datetime)
        from time                               import sleep
        from urllib                             import quote_plus,unquote
        from re                                 import findall          as re_findall
        from re                                 import sub              as re_sub           # re_sub('patt','repl','str','cnt')
        from re                                 import search           as re_search        # re_search('patt','str')
        import json
        from subprocess                         import Popen            as sub_popen
        from subprocess                         import PIPE             as sub_PIPE
        from traceback                          import format_exc       as tb_format_exc
        from sys                                import exc_info         as sys_exc_info
        from types                              import NoneType
        from time                               import sleep            as delay
        from uuid                               import uuid4            as get_guid
        import                                  requests

        from py_classes                         import To_Sub_Classes,To_Class,To_Class_Dict
        T                                   =   To_Class()

        for k,v in kwargs.iteritems():
            T[k] = v

        db_vars = ['DB_NAME','DB_HOST','DB_PORT','DB_USER','DB_PW']
        db_vars = [it for it in db_vars if not T._has_key(it)]

        if locals().keys().count('db_settings'):
            DB_NAME,DB_USER,DB_PW,DB_HOST,DB_PORT = db_settings
            for it in db_vars:
                eval('T["%s"] = %s' % (it,it))
            
        else:
            z = eval("__import__('db_settings')")
            for it in db_vars:
                T[it] = getattr(z,it)
        
        import                                  pandas                  as pd
        pd.set_option(                          'expand_frame_repr', False)
        pd.set_option(                          'display.max_columns', None)
        pd.set_option(                          'display.max_colwidth', 250)
        pd.set_option(                          'display.max_rows', 1000)
        pd.set_option(                          'display.width', 1500)
        pd.set_option(                          'display.colheader_justify','left')
        np                                  =   pd.np
        np.set_printoptions(                    linewidth=1500,threshold=np.nan)
        # import                                  geopandas               as gd
        import logging
        logger = logging.getLogger(                      'sqlalchemy.dialects.postgresql')
        logger.setLevel(logging.INFO)
        from sqlalchemy                         import create_engine
        from psycopg2                           import connect          as pg_connect
        try:
            eng                             =   create_engine(r'postgresql://%(DB_USER)s:%(DB_PW)s@%(DB_HOST)s:%(DB_PORT)s/%(DB_NAME)s'
                                                              % T,
                                                              encoding='utf-8',
                                                              echo=False)
            conn                            =   pg_connect("dbname='%(DB_NAME)s' host='%(DB_HOST)s' port=%(DB_PORT)s \
                                                           user='%(DB_USER)s' password='%(DB_PW)s' "
                                                           % T);
            cur                             =   conn.cursor()

        except:
            from getpass import getpass
            pw = getpass('Root password (to create DB:"%(DB_NAME)s" via CL): ' % pgsql)
            p = sub_popen(" ".join(["echo '%s' | sudo -S prompt='' " % pw,
                                    'su postgres -c "psql --cluster 9.4/main -c ',
                                    "'create database %(DB_NAME)s;'" % T,
                                    '"']),
                          stdout=sub_PIPE,
                          shell=True)
            (_out, _err) = p.communicate()
            assert _err is None

            eng                             =   create_engine(r'postgresql://%(DB_USER)s:%(DB_PW)s@%(DB_HOST)s:%(DB_PORT)s/%(DB_NAME)s'
                                                              % T,
                                                              encoding='utf-8',
                                                              echo=False)
            conn                            =   pg_connect("dbname='%(DB_NAME)s' host='%(DB_HOST)s' port=%(DB_PORT)s \
                                                           user='%(DB_USER)s' password='%(DB_PW)s' "
                                                           % T);
            cur                             =   conn.cursor()


        import inspect, os
        D                                   =   {'guid'                 :   str(get_guid().hex)[:7],
                                                 'pg_classes_pwd'                  :   os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                                }
        D.update(                               {'tmp_tbl'              :   'tmp_'+D['guid'],
                                                 'current_filepath'     :   inspect.getfile(inspect.currentframe())})



        self.T                              =   To_Class_Dict(  self,
                                                                dict_list=[T.__dict__,D,locals()],
                                                                update_globals=True)

        self.Functions                      =   pgSQL_Functions(self)
        self.Triggers                       =   pgSQL_Triggers(self)
        self.Tables                         =   pgSQL_Tables(self)
        self.Types                          =   pgSQL_Types(self)
        self.__initial_check__(                 )
        self.__temp_options__(                  )

    def __initial_check__(self):
        # at minimum, confirm that geometry is enabled
        self.F.functions_run_confirm_extensions(verbose=False)
        self.F.functions_create_batch_groups(grps=['admin'])

        if not self.F.triggers_exists_event_trigger('missing_primary_key_trigger'):
            print 'missing missing_primary_key_trigger'
            # idx_trig = raw_input('add trigger to automatically create column "uid" as index col if table created without index column? (y/n)\t')
            # if idx_trig=='y':
            #     self.F.triggers_create_z_auto_add_primary_key()

        if not self.F.triggers_exists_event_trigger('missing_last_updated_field'):
            print 'missing missing_last_updated_field'
            # modified_trig = raw_input('add trigger to automatically create column "last_updated" for all new tables and update col/row when row modified? (y/n)\t')
            # if modified_trig=='y':
            #     self.F.triggers_create_z_auto_add_last_updated_field()
                #### self.F.triggers_create_z_auto_update_timestamp()

    def __temp_options__(self):

        #self.T.redirect_logs_to_file(                  '/tmp/tmplog')

        pass

