# from ipdb import set_trace as i_trace
# i_trace() 
exec ""

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

        def confirm_extensions(self,exts=['plpythonu','pllua','plshu'],verbose=False):
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

        def get_function_info(self,func):
            qry = """
                SELECT 
                    proname f_name, 
                    prolang f_lang,
                    proisagg is_agg,
                    CASE
                    WHEN provolatile='v' THEN 'true'
                    ELSE 'false'
                    END as is_volatile,
                    proallargtypes all_arg_type,
                    proargtypes arg_types,
                    pronargdefaults arg_defaults,
                    prorettype return_type,
                    prosrc src
                FROM pg_proc 
                WHERE proname ilike '%s'
                """ % func
            return                              self.T.pd.read_sql(qry,self.T.eng)

        def get_general_function_info(self):
            q = """
                SELECT n.nspname as "Schema",
                    p.proname as "f_name",
                    pg_catalog.pg_get_function_result(p.oid) as "result_type",
                    pg_catalog.pg_get_function_arguments(p.oid) as "arg_types",
                    CASE
                        WHEN p.proisagg THEN 'agg'
                        WHEN p.proiswindow THEN 'window'
                        WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
                        ELSE 'normal'
                    END as "f_type"
                FROM pg_catalog.pg_proc p
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE pg_catalog.pg_function_is_visible(p.oid)
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname <> 'information_schema'
                ORDER BY 1, 2, 4;
                """
            return                              self.T.pd.read_sql(q,self.T.eng)

    class Create:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def __show_auto_groups(self):
            (_out,_err) = self.T.sub_popen('ls -p %s/sql_functions/ | grep "/"' % self.T.pg_classes_pwd,
                   stdout=self.T.sub_PIPE,
                   shell=True).communicate()

            assert _err is None
            print _out            

        def from_command_line(self,**kwargs):
            """
                :param one_directory: The absolute directory path or directory path relative to the primary module 
                from which the files are loaded in simple sort order. NOTE. IF DEFINED, OTHER PATH PARAMS IGNORED.
                :type one_directory: str.

                :param one_file: The absolute file path or file path relative to the primary module. NOTE. IF DEFINED, OTHER PATH PARAMS IGNORED.
                :type one_file: str.

                :param sub_dir: The relative directory to this file.
                :type sub_dir: str.

                :param grps: A sequence list of relative subdirectories for loading files.
                :type grps: list.

                :param files: A specific list of files to only load.
                :type files: list.

                :param regex_exclude: A list of files matching provided regex and excluded from loading.
                :type files: list.         

                :returns:  None

            """
            def get_psql_path():
                p = self.T.sub_popen('; '.join(['unalias psql > /dev/null 2>&1',
                                                'unset psql > /dev/null 2>&1',
                                                'which psql']),
                                      stdout=self.T.sub_PIPE,
                                      shell=True)
                (_out, _err) = p.communicate()
                assert _err is None
                return _out.rstrip('\n')

            # Defaults:
            one_file = None if not kwargs.has_key('one_file') else kwargs['one_file']
            one_directory = None if not kwargs.has_key('one_directory') else kwargs['one_directory']
            sub_dir = 'sql_functions'
            grps = ['admin','json','strings']
            files = ['all']
            regex_exclude = [r'(?iLmsux).*/(_[^\/]+)[.]sql$']


            for name, value in kwargs.iteritems():
                # import ipdb as I; I.set_trace()
                if not value.isdigit():
                    exec '%s = "%s"' % (name, value) in globals(),locals()
                else:
                    exec '%s = %s' % (name, value) in globals(),locals()

            cmds = []
            # cmd_template = ''.join(['psql -d '+self.T.DB_NAME+' -c "%s ' + '%s/%s' % (self.T.pg_classes_pwd,sub_dir) + '/%s/%s;"'
            # sh_cmd_template = 'psql --dbname=%(DB_NAME)s --host=%(DB_HOST)s --port=%(DB_PORT)s --username=%(DB_USER)s --command="%(COMMAND)s"'
            sh_cmd_template = ' '.join(['%(PSQL_PATH)s --dbname=%(DB_NAME)s --host=%(DB_HOST)s',
                                        '--port=%(DB_PORT)s --username=%(DB_USER)s --file=%(FPATH)s'])
            D = self.T.config.__dict__
            # D['PSQL_PATH'] = get_psql_path()
            D['PSQL_PATH'] = "/usr/local/bin/psql"

            if one_directory: 
                dir_list=[]
                for root, sub_dirs, files in self.T.os.walk(self.T.os.path.abspath(one_directory)):
                    for f in files:
                        if not regex_exclude:
                            dir_list.append(self.T.os.path.join(root,f))
                        else:
                            fpath = self.T.os.path.join(root,f)
                            for regex in regex_exclude:
                                if type(self.T.re.match(regex,fpath))==self.T.NoneType:
                                    dir_list.append(fpath)
                        
                fpaths = sorted(dir_list)
                
                for it in fpaths:
                    D['FPATH'] = it
                    cmds.append( sh_cmd_template % D)

            elif one_file:
                D['FPATH'] = self.T.os.path.abspath(one_file)
                cmds.append( sh_cmd_template % D)

            else:
                files = files if type(files)==list else list(files)
                for d in grps:
                    for f in sorted(self.T.os.listdir('%s/%s/%s' % (self.T.pg_classes_pwd,sub_dir,d))):
                        if f.count('.sql'):
                            if files.count(f) or files==['all']:

                                # cmds.append( cmd_template % ('\\\\\\\\i',d,f) )

                                D['FPATH'] = ''.join([  '%s/%s' % (self.T.pg_classes_pwd,sub_dir),
                                                        '/%s/%s' % (d,f) ])
                                cmds.append( sh_cmd_template % D )

            # -----------

            cmds = '\n'.join(cmds)
            qry = """
                DO E'#!/bin/sh

                    export PGOPTIONS="--client-min-messages=warning"
                    %s
                    unset PGOPTIONS
                    ' LANGUAGE plshu;
                """ % cmds
            self.T.to_sql(qry)
            

        def z_next_free(self):
            self.F.functions_destroy_z_next_free()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['admin'],
                                                    files=['1_z_next_free.sql'])
        def z_get_seq_value(self):
            self.F.functions_destroy_z_get_seq_value()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['admin'],
                                                    files=['2_z_get_seq_value.sql'])
        def z_array_sort(self):
            self.F.functions_destroy_z_array_sort()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['admin'],
                                                    files=['5_z_array_sort.sql'])
        def z_make_column_primary_serial_key(self):
            self.F.functions_destroy_z_make_column_primary_serial_key()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['admin'],
                                                    files=['6_z_make_column_primary_serial_key.sql'])

        def json_functions(self):
            self.F.functions_destroy_json_functions()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['json'],
                                                    files=['all'])
        def string_functions(self):
            self.F.functions_destroy_string_functions()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['string'],
                                                    files=['all'])

    class Destroy:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def z_next_free(self):
            c                               =   """
                DROP FUNCTION IF EXISTS z_next_free(text, text, text) CASCADE;
                                            """
            self.T.to_sql(                      qry)

        def z_get_seq_value(self):
            c                               =   """
                DROP FUNCTION IF EXISTS z_get_seq_value(text) CASCADE;
                                            """
            self.T.to_sql(                      qry)

        def z_array_sort(self):
            c                               =   """
                DROP FUNCTION IF EXISTS z_array_sort(anyarray) CASCADE;
                                            """
            self.T.to_sql(                      qry)

        def z_make_column_primary_serial_key(self):
            c                               =   """
                DROP FUNCTION IF EXISTS 
                    z_make_column_primary_serial_key(text,text,boolean,boolean,boolean) CASCADE;
                                            """
            self.T.to_sql(                      qry)

        def json_functions(self):
            q,q_temp = [],'DROP FUNCTION IF EXISTS %s(%s) CASCADE;'
            df = self.F.functions_run_get_general_function_info()
            if len(df):
                for i,r in df.iterrows():
                    if str(r.f_name).find('json_')==0:
                        arg_types = str([str(it.split()[1]) for it in r.arg_types.split(',')]).strip('[]').replace("'",'')
                        q.append(q_temp % (r.f_name,arg_types))
                qry = ' '.join(q)
                self.T.to_sql(                      qry)

        def string_functions(self):
            q,q_temp = [],'DROP FUNCTION IF EXISTS %s(%s) CASCADE;'
            df = self.F.functions_run_get_all_functions()
            for i,r in df.iterrows():
                if str(r.f_name).find('z_str_')==0:
                    arg_types = str([str(it.split()[1]) for it in r.arg_types.split(',')]).strip('[]').replace("'",'')
                    q.append(q_temp % (r.f_name,arg_types))
            qry = ' '.join(q)
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
        def tbl_trigger(self,trigger_name,tbl_name):
            qry                             =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_trigger t
                                                    INNER JOIN pg_class c 
                                                    ON t.tgrelid=c.relfilenode
                                                    WHERE NOT tgisinternal
                                                    AND relname='%s'
                                                    AND tgname ilike '%s');
                                                """ % (trigger_name,tbl_name)
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
        def tbl_trigger(self,trigger_name,tbl_name):
            qry                             =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_trigger t
                                                    INNER JOIN pg_class c 
                                                    ON t.tgrelid=c.relfilenode
                                                    WHERE NOT tgisinternal
                                                    AND relname='%s'
                                                    AND tgname ilike '%s'
                                                    AND tgenabled = 'O');
                                                """ % (trigger_name,tbl_name)
            return                              self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Create:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def z_auto_add_primary_key(self):
            self.F.triggers_destroy_z_auto_add_primary_key()
            
            self.F.functions_create_z_next_free()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['admin'],
                                                    files=['3_z_auto_add_primary_key.sql'])
        def z_auto_add_last_updated_field(self):
            self.F.triggers_destroy_z_auto_add_last_updated_field()
            self.F.functions_create_batch_groups(sub_dir='sql_functions',
                                                    grps=['admin'],
                                                    files=['4_z_auto_add_last_updated_field.sql'])
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
                DROP FUNCTION IF EXISTS z_auto_add_primary_key() CASCADE;
                DROP EVENT TRIGGER IF EXISTS missing_primary_key_trigger CASCADE;
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)
        def z_auto_add_last_updated_field(self):
            c                               =   """
                DROP FUNCTION IF EXISTS z_auto_add_last_updated_field() CASCADE;
                DROP EVENT TRIGGER IF EXISTS missing_last_updated_field CASCADE;
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)

    class Show:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def event_triggers(self):
            cmd                             =   "select * from pg_event_trigger"
            self.T.to_sql(                      cmd)


    class Operate:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def disable_tbl_trigger(self,tbl,trigger_name):
            cmd = "ALTER TABLE %(tbl)s DISABLE TRIGGER %(trig)s;" % {'tbl':tbl,'trig':trigger_name}
            self.T.to_sql(                      cmd)
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

    def get_info(self,tbl_name):
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

                                                """ % (tbl_name,tbl_name)
        return                                  self.T.pd.read_sql(qry,self.T.eng)

    def get_triggers(self,tbl_name):
        qry                                 =   """
                                                SELECT  c.relname tbl_name,
                                                        t.tgname trigger_name,
                                                        CASE
                                                            WHEN t.tgenabled = 'O' THEN true
                                                            ELSE false
                                                        END AS is_enabled,
                                                        tgattr column_triggers,
                                                        tgnargs arg_str_cnt,
                                                        tgargs arg_strs
                                                FROM pg_trigger t
                                                INNER JOIN pg_class c 
                                                ON t.tgrelid=c.relfilenode
                                                WHERE NOT tgisinternal
                                                AND relname='%s';
                                                """ % tbl_name
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
                                                        pg_index.indisprimary = 't' AND
                                                        NOT pg_class.relname ILIKE 'pg_%%';
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

class pgSQL_Databases:

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    def get_all(self):
        qry                                 =   """
                                                SELECT datname FROM pg_database
                                                WHERE datistemplate = false

                                                """
        return                                  self.T.pd.read_sql(qry,self.T.eng)

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

        def run_cmd(cmd):
            p = sub_popen(cmd,stdout=sub_PIPE,
                          shell=True,
                          executable='/bin/bash')
            (_out,_err) = p.communicate()
            assert _err is None
            return _out.rstrip('\n')

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
            # print(cmd)
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

        def _load_connectors():
            eng                             =   create_engine(r'postgresql://%(DB_USER)s:%(DB_PW)s@%(DB_HOST)s:%(DB_PORT)s/%(DB_NAME)s'
                                                              % T,
                                                              encoding='utf-8',
                                                              echo=False)
            conn                            =   pg_connect("dbname='%(DB_NAME)s' host='%(DB_HOST)s' port=%(DB_PORT)s \
                                                           user='%(DB_USER)s' password='%(DB_PW)s' "
                                                           % T);
            cur                             =   conn.cursor()
            return eng,conn,cur

        import                                  datetime                as DT
        dt = DT
        from dateutil                           import parser           as DU               # e.g., DU.parse('some date as str') --> obj(datetime.datetime)
        import                                  time
        delay                               =   time.sleep
        from urllib                             import quote_plus,unquote
        import re
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
        T                                   =   To_Class(kwargs,recursive=True)
        
        # PUT ALL KWARGS AS T.config
        # T.config                            =   To_Class(kwargs,recursive=True)

        if hasattr(T,'config') and hasattr(T.config,'pgsql'): 
            T.__dict__.update(                  T.config.pgsql.__dict__)
        if hasattr(T,'config'):
            T.__dict__.update(                  T.config.__dict__)
        if hasattr(T,'db_settings'):
            T.__dict__.update(                  T.db_settings.__dict__)

        db_vars = ['DB_NAME','DB_HOST','DB_PORT','DB_USER','DB_PW']
        db_vars = [it for it in db_vars if not T._has_key(it)]

        # import ipdb as I; I.set_trace()

        if not db_vars:
            pass

        elif locals().keys().count('db_settings'):
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
        logger = logging.getLogger(             'sqlalchemy.dialects.postgresql')
        logger.setLevel(logging.INFO)
        from sqlalchemy                         import create_engine
        from psycopg2                           import connect          as pg_connect
        try:
            eng,conn,cur                    =   _load_connectors()

        except:
            from getpass import getpass
            pw = getpass('Root password (to create DB:"%(DB_NAME)s" via CL): ' % T)
            p = sub_popen(" ".join(["echo '%s' | sudo -S prompt='' " % pw,
                                    'su postgres -c "psql --cluster 9.4/main -c ',
                                    "'create database %(DB_NAME)s;'" % T,
                                    '"']),
                          stdout=sub_PIPE,
                          shell=True)
            (_out, _err) = p.communicate()
            assert _err is None
            eng,conn,cur                    =   _load_connectors()


        import inspect, os
        D                                   =   {'guid'                 :   str(get_guid().hex)[:7],
                                                 'pg_classes_pwd'       :   os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                                }
        D.update(                               {'tmp_tbl'              :   'tmp_'+D['guid'],
                                                 'current_filepath'     :   inspect.getfile(inspect.currentframe())})

        self.T                              =   To_Class_Dict(  self,
                                                                dict_list=[T.__dict__,D,locals()],
                                                                update_globals=True)

        self.Functions                      =   pgSQL_Functions(self)
        self.Triggers                       =   pgSQL_Triggers(self)
        self.Tables                         =   pgSQL_Tables(self)
        self.Databases                      =   pgSQL_Databases(self)
        self.Types                          =   pgSQL_Types(self)

        # if hasattr(T,'project_sql_files') and T.project_sql_files:
        #     self.F.functions_create_from_command_line(one_directory=T.project_sql_files)
        # if hasattr(T,'base_sql_files') and T.base_sql_files:
        #     self.F.functions_create_from_command_line(one_directory=T.base_sql_files)  
        if hasattr(T,'initial_check') and T.initial_check:
            self.__initial_check__()
        if hasattr(T,'temp_options') and T.temp_options:
            self.__temp_options__()

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

