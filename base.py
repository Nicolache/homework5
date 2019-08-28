import sqlite3


def create_sqlite3_session(filename):

    return sqlite3.connect(filename)


class MetaType(type):

    def __str__(self):
        return self.__name__


class Integer(metaclass=MetaType):

    pass


class String(metaclass=MetaType):

    def __init__(self, length=100):
        self.length = length

    def __str__(self):
        return f'Varchar({self.length})'


class Field(object):

    _template_definition = '{type} {not_null} {primary_key} {autoincrement}'
    _template_foreign_definition = \
        'FOREIGN KEY ({self_name}) references {table}({field})'

    def __init__(
        self,
        type=None,
        primary=False,
        autoincrement=False,
        not_null=True,
        foreign=None,
        default_value=None,
    ):
        if not type:
            raise Exception('Please, specify type of column')
        else:
            self.type = type

        self.primary = primary
        self.autoincrement = autoincrement
        self.not_null = not_null
        self.foreign = foreign
        self.default_value = default_value
        self.table_class = None
        self._fk = None

    
    @classmethod
    def value(cls, val):
        return val or 'NULL'

    @property
    def full_name(self):
        return f'{self.tablename}.{self.name}'

    @property
    def as_name(self):
        return f'{self.tablename}_{self.name}'

    @property
    def definition(self):
        """Returns a formatted string applying the output of _definition_dict
        function to unformatted string _template_definition.
        """
        return self._template_definition.format(**self._definition_dict)

    @property
    def _definition_dict(self):
        """Returns a dictionary of query parameters that is used
        for applying to string like:
        _template_definition = '{type} {not_null} {primary_key} {autoincrement}'
        and building a query string.
        """
        return {
            'type': self.type,
            'not_null': 'NOT NULL' if self.not_null else '',
            'primary_key': 'PRIMARY KEY' if self.primary else '',
            'autoincrement': 'AUTOINCREMENT' if self.autoincrement else '',
        }

    @property
    def foreign_key_definition(self):
        if self.foreign:
            return self._template_foreign_definition.format(
                self_name=self.name,
                table=self.foreign.tablename,
                field=self.foreign.name
            )

    @property
    def name(self):
        return self.table_class.get_field_name(self)

    @property
    def tablename(self):
        return self.table_class.__tablename__

    def set_table_class(self, cls):
        self.table_class = cls


class MetaBase(type):
    """This metaclass is used for the "Base" class creation. The "type" class
    is inherited into this metaclass. The "type" class is used for classes
    creation in python.
    The common purpose of usage metaclasses is for dynamic classes changing
    at the time of its creation. It this particular case,
    it sets table class attribute for every field in this class.
    """
    def __init__(cls, name, bases, attrs):
        """
        Keyword arguments:
        cls -- it's like "self" in an usual method.
        name -- class name.
        bases -- inherited classes.
        attrs -- attributes of the class. A dictionary of class method objects.
        """
        super().__init__(name, bases, attrs)

        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, Field):
                attr_value.set_table_class(cls)
                if attr_value.foreign:
                    attr_value.foreign.__setattr__('_fk', cls)


class Base(metaclass=MetaBase):

    _create_table_template = 'create table if not exists {table} ({fields} {foreign_keys})'
    _drop_table_template = 'drop table if exists {table}'
    _insert_template = 'insert into {table}({fields}) values ({values})'
    _select_template = 'select {fields} from {table} {joins}'
    _update_template = 'update {table} set {values}'
    _delete_template = 'delete from {table}'
    _join_template = 'left join {fk_table} on {fk_table}.{fk} = {pk_table}.{pk}'
    # _select_template = 'select {fields} from {table}'

    _session = None  # session inside class
    __tablename__ = None
    _filter_str = ''

    def __init__(self, **kwargs):
        super().__init__()
        for field_name, field_instance in self.__class__.get_fields():
            self.__setattr__(field_name, kwargs.get(field_name, field_instance.default_value))

    @classmethod
    def filter(cls, condition):

        cls._filter_str = str(condition)
        return cls

    @classmethod
    def set_session(cls, session):

        cls._session = session

    @classmethod
    def get_session(cls):

        if cls._session is not None:
            return cls._session
        else:
            raise Exception('Cannot get session.')

    @classmethod
    def get_cursor(cls):

        return cls.get_session().cursor()

    @classmethod
    def get_field_name(cls, field):

        for name, instance in cls.__dict__.items():
            if instance is field:
                return name

    @classmethod
    def get_fields(cls):
        """
        cls.__dict__.items().{keys}: id, name, email; id, user_id, post
        cls.__dict__.items().{values}: base.Field object
        cls.__dict__.items().{values}.definition: string
        """
        for name, field in cls.__dict__.items():
            if isinstance(field, Field):
                yield name, field

    @classmethod
    def get_fields_dict(cls):

        return {
            field.full_name: field.__class__.value(getattr(cls, name))
            for name, field in cls.get_fields()
        }

    @classmethod
    def get_foreign_field_by_table(cls, table):

        for name, field in cls.get_fields():
            if field.foreign and field.foreign.table_class is table:
                return field

    @classmethod
    def get_field_definitions(cls):
        """Returns a string like:
        id Integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        name Varchar(100) NOT NULL  ,
        email Varchar(100) NOT NULL  
        id Integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        user_id Integer NOT NULL  , post Varchar(100)
        """
        definitions_str = f''
        for name, field in cls.get_fields():
            definitions_str += f'{name} {field.definition}, '
        definitions_str = definitions_str[:-2]
        return definitions_str

    @classmethod
    def get_keys(cls):
        """Returns a string like:
        , FOREIGN KEY (user_id) references users(id)
        """
        keys = f''
        for name, field in cls.get_fields():
            if field.foreign_key_definition:
                keys += f', {field.foreign_key_definition}'
        return keys


    @classmethod
    def create_table(cls):
        """Returns a string of a complete query.
        """
        fields_definition = cls.get_field_definitions()
        keys = cls.get_keys()
        query = cls._create_table_template.format(
            table=cls.__tablename__,
            fields=fields_definition,
            foreign_keys=keys,
        )
        # print(query)
        cls.get_cursor().execute(query)

    @classmethod
    def drop_table(cls):

        query = cls._drop_table_template.format(
            table = cls.__tablename__
        )
        cls.get_cursor().execute(query)

    def save(cls):

        query = cls._insert_template.format(
            table = cls.__tablename__,
            fields = ', '.join(cls.__dict__.keys()),
            values = ', '.join(
                f'"{value}"' if value else 'NULL'
                for value in cls.__dict__.values()
            ),
        )
        try:
            cursor = cls.get_cursor()
            cursor.execute(query)
            cls.get_session().commit()
        except Exception as e:
            print(f'sqlite error: {e}')
            return

        last_id = cursor.lastrowid
        for name, field in cls.get_fields():
            if field.primary:
                cls.__setattr__(name, last_id)

    @classmethod
    def update(cls, **kwargs):

        fields = {
            key: cls.__dict__.get(key).__class__.value(value)
            for key, value in kwargs.items()
        }

        query = cls._update_template.format(
            table = cls.__tablename__,
            values = ', '.join(f'{field} = "{value}"' for field, value in fields.items())
        )

        try:
            result = cls.get_cursor().execute(query).fetchall()
            cls.get_session().commit()
        except Exception as e:
            print(f'sqlite error: {e}')
        return cls

    @classmethod
    def join(cls):

        for name, field in cls.get_fields():
            if field._fk and field.table_class == cls:
                fk_table = field._fk
                yield {
                    'fk_table': fk_table.__tablename__,
                    'fk': fk_table.get_foreign_field_by_table(cls).name,
                    'pk_table': cls.__tablename__,
                    'pk': field.name,
                }
 
    @classmethod
    def get(cls, *args):

        fk_fields = {}

        for name, field in cls.get_fields():
            if field._fk and field.table_class == cls:
                fk_table = field._fk
                # fk_fields = dict(fk_table.get_fields_dict())
                fk_fields = dict(fk_table.get_fields())
        print(fk_table)
        print(fk_fields)

        # all_fields = dict(cls.get_fields_dict())
        all_fields = dict(cls.get_fields())
        all_fields.update(fk_fields)

        fields = {name: field for name, field in all_fields.items() if name in args}

        if not fields:
            fields = all_fields

        query = cls._select_template.format(
            table = cls.__tablename__,
            fields = ', '.join(f'{name} as {field.as_name}' for name, field in fields.items()),
            joins = ' '.join(cls._join_template.format(**line) for line in cls.join()),
        )
        print(query)

        try:
            result = cls.get_cursor().execute(query).fetchall()
            cls.get_session().commit()
        except Exception as e:
            print(f'sqlite error: {e}')

        return cls

#     @classmethod
#     def get(self, cls, *args):
# 
#         self._cls = cls
#         fk_fields = {}
# 
#         for name, field in cls.get_fields():
#             if field._fk and field.table_class == cls:
#                 fk_table = field._fk
#                 fk_fields = dict(fk_table.get_fields_dict())
# 
#         all_fields = dict(cls.get_fields_dict())
#         all_fields.update(fk_fields)
# 
#         fields = {name: field for name, field in all_fields.items() if name in args}
# 
#         if not fields:
#             fields = all_fields
# 
#         self._query = self._select_template.format(
#             table = cls.__tablename__,
#             fields = ', '.join(f'{name} as {field.as_name}' for name, field in fields.items()),
#             joins = ' '.join(self._join_template.format(**line) for line in self.join()),
#         )
#         print(self._query)
# 
#         return self

    def delete(cls):

        query = _delete_template.format(
            table = cls.__tablename__,
        )
        cls.get_cursor().execute(query)
