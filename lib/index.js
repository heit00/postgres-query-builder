class QueryBuilder {

    /***
     *  ATRIBUTES ARE CREATED IN RUNTIME;
     */
    static operatorConditional = ['>', '<', '=', '!=', '<=', '>=', '<>', 'IN'];
    static joinTypes = {
        innerJoin: 'INNER JOIN',
        leftJoin: 'LEFT JOIN',
        rightJoin: 'RIGHT JOIN',
        fullOuterJoin: 'FULL OUTER JOIN',
        naturalJoin: 'NATURAL JOIN'
    };
    static logicalOperator = {
        or: 'OR',
        and: 'AND',
    };
    static orderByTypes = {
        asc: 'ASC',
        desc: 'DESC'
    }

    orderby = [];
    joins = [];
    wheres = [];
    groupby = [];
    have = [];

    select(...columns) {
        if (this.action) throw new SyntaxError(`${this.action} is already regitered as action.`);
        this.action = 'SELECT';

        if (columns.length === 1) {
            if (Array.isArray(columns[0])) {
                columns = columns[0];
            }
            else if (typeof columns[0] === 'object') {
                const interable = columns[0]
                const finalObjs = [];
                for (let key of Object.keys(interable)) {
                    if (typeof interable[key] !== 'string') throw new TypeError('object must have only string as values.');
                    else finalObjs.push({ table: interable[key], alias: key || undefined });
                }
                this.selectColumns = finalObjs
                return;
            }

        }

        this.selectColumns = columns.map(el => {
            if (typeof el === 'string') {
                return { table: el, alias: undefined };
            }
            else if (typeof el === 'object') {
                if (Object.keys(el).length !== 1) throw new TypeError('object must have only 1 property.');
                const key = Object.keys(el)[0];
                if (typeof el[key] !== 'string') throw new TypeError(('object must have only strings as property.'));
                return { table: el[key], alias: key };
            }
            else throw new TypeError('must be an object or string.');
        })

        return this;
    }

    from(...tables) {
        if (this.fromTables) throw new SyntaxError('from was already used');
        if (tables.length === 0) throw new SyntaxError('from() requires at least one table');

        if (tables.length === 1 && typeof tables[0] === 'object' && !Array.isArray(tables[0])) {
            tables = tables[0];
            const finalObjs = [];
            for (let key of Object.keys(tables)) {
                if (typeof tables[key] == 'string') {
                    finalObjs.push({ table: tables[key], alias: key })
                }
                else if (tables[key] instanceof QueryBuilder) {
                    if (!key) throw new SyntaxError('alias is mandatory.');
                    finalObjs.push({ table: `(${tables[key].toInstruction()})`, alias: key })
                }
                else {
                    throw new TypeError('tables propertys must contain only strings.');
                }
            }

            this.fromTables = finalObjs;
            return this;
        }

        if (tables.length === 1 && Array.isArray(tables[0])) {
            tables = tables[0];
        }

        this.fromTables = tables.map(el => {
            if (typeof el === 'string')
                return { table: el, alias: undefined };
            else if (typeof el === 'object' && !Array.isArray(el)) {
                if (Object.keys(el).length !== 1) throw new SyntaxError('Object must have 1 property.');
                const key = Object.keys(el)[0];
                if (el[key] instanceof QueryBuilder) {
                    if (!key) throw new SyntaxError('alias is mandatory.');
                    return { table: el[key].toInstruction(), alias: key };
                }
                if (typeof el[key] !== 'string') throw new TypeError('objects must have only strings as values.');
                return { table: el[key], alias: key };
            }
            else throw new TypeError('tables must contain strings or objects.');
        });

        return this;
    }

    join({ joinType = QueryBuilder.joinTypes.innerJoin, config, value1, operator = '=', value2 } = {}) {
        const isNatural = joinType === QueryBuilder.joinTypes.naturalJoin;
        if (!isNatural && (!value1 || (typeof value2 === 'string' && value2.trim().length === 0))) throw new SyntaxError('value1, operator and value2 must be defined for non-naturalJoins.');
        if (typeof operator === 'string') operator = operator.toUpperCase();
        if (typeof joinType === 'string') joinType = joinType.toUpperCase();
        if (!isNatural && !QueryBuilder.operatorConditional.includes(operator)) throw new TypeError('operator must be in ' + QueryBuilder.operatorConditional.join(', '));
        if (!Object.values(QueryBuilder.joinTypes).includes(joinType)) throw new SyntaxError('join type must be in: { ' + Object.values(QueryBuilder.joinTypes).join(', ') + ' }');
        if (isNatural && (operator || value1 || value2)) throw new SyntaxError('natural join does not alow conditions.');
        let finalObj = {};
        if (typeof config === 'string') {
            finalObj = { table: config, alias: undefined, operator, value2, type: joinType };
        }
        else if (typeof config === 'object' && !Array.isArray(config)) {
            if (Object.keys(config).length !== 1) throw new TypeError('object must have only 1 property.');
            const key = Object.keys(config)[0];
            let table
            if (config[key] instanceof QueryBuilder) {
                if (!key) throw new SyntaxError('alias is mandatory.');
                table = `(${config[key].toInstruction()})`;
            }
            else if (typeof config[key] === 'string') {
                table = config[key]
            }

            if (!isNatural)
                finalObj = { table: table, alias: key || undefined, operator, value1, value2, type: joinType };
            else
                finalObj = { table: table, alias: key };
        }

        const duplicated = this.joins.some(el => el.table === finalObj.table && el.alias === finalObj.alias);
        if (duplicated) throw new SyntaxError('join already registered.');

        this.joins.push(finalObj);
        return this;
    }

    where({ column, operator = '=', value, logicalOperator = 'AND' } = {}) {
        if (typeof value === 'string' && value.trim().length === 0) throw new SyntaxError('value must be defined');
        if (typeof logicalOperator !== 'string') throw new TypeError('logical operator must be a tring.');
        logicalOperator = logicalOperator.toLocaleUpperCase();
        if (typeof operator === 'string') operator = operator.toUpperCase();
        if (!Object.values(QueryBuilder.logicalOperator).includes(logicalOperator)) throw new SyntaxError('logicalOperator must be in: ( ' + Object.values(QueryBuilder.logicalOperator).join(', ') + ' )');
        if (typeof column !== 'string') throw new TypeError('column must be a string.');
        if (!QueryBuilder.operatorConditional.includes(operator)) throw new TypeError('operator must be in: ' + QueryBuilder.operatorConditional.join(', '));

        this.wheres.push({ column, operator, value, logicalOperator });
        return this;
    }

    orWhere({ column, operator = '=', value } = {}) {
        if (typeof value === 'string' && value.trim().length === 0) throw new SyntaxError('value must be defined');
        if (typeof operator === 'string') operator = operator.toUpperCase();
        if (typeof column !== 'string') throw new TypeError('column must be a string.');
        if (!QueryBuilder.operatorConditional.includes(operator)) throw new TypeError('operator must be in: ' + QueryBuilder.operatorConditional.join(', '));

        this.wheres.push({ column, operator, value, logicalOperator: 'OR' });
        return this;
    }

    having(column, operator, value, logicalOperator = 'AND') {
        if (typeof operator === 'string') operator = operator.toUpperCase();
        if (typeof logicalOperator !== 'string') throw new TypeError('logical operator must be a tring.');
        logicalOperator = logicalOperator.toLocaleUpperCase();
        if (!Object.values(QueryBuilder.logicalOperator).includes(logicalOperator)) throw new SyntaxError('logicalOperator must be in: ( ' + Object.values(QueryBuilder.logicalOperator).join(', ') + ' )');
        if (typeof column !== 'string') throw new TypeError('column must be a string.');
        if (!QueryBuilder.operatorConditional.includes(operator)) throw new TypeError('operator must be in: ' + QueryBuilder.operatorConditional.join(', '));

        this.have.push({ column, operator, value, logicalOperator });
        return this;
    }

    orHaving(column, operator, value) {
        if (typeof operator === 'string') operator = operator.toUpperCase();
        if (typeof column !== 'string') throw new TypeError('column must be a string.');
        if (!QueryBuilder.operatorConditional.includes(operator)) throw new TypeError('operator must be in: ' + QueryBuilder.operatorConditional.join(', '));

        this.have.push({ column, operator, value, logicalOperator: 'OR' });
        return this;
    }

    distinct() {
        if (this.dist) throw new SyntaxError('distinct alread registered.');
        this.dist = true;
        return this;
    }

    orderBy(...columns) {
        if (columns.length === 0) throw new SyntaxError('columns can not be empty.');
        if (columns.length == 1 && Array.isArray(columns[0])) {
            columns = columns[0];
        }

        const finalObjs = [];

        columns.forEach(el => {
            if (typeof el === 'string') {
                finalObjs.push({ column: el, order: 'ASC' });
            }
            else if (typeof el === 'object' && !Array.isArray(el) && el.column) {
                if (typeof el.column !== 'string') throw new TypeError('colums objects must contain only strings.');
                if (el.order !== undefined && (typeof el.order !== 'string' || !Object.values(QueryBuilder.orderByTypes).includes(el.order.toUpperCase())))
                    throw new SyntaxError('.order must be in ' + Object.values(QueryBuilder.orderByTypes).join(', '));
                finalObjs.push({ column: el.column, order: (el.order || 'ASC').toUpperCase() });
            }
            else throw new SyntaxError('columns must contains strings or objects.');
        });

        this.orderby = [...this.orderby, ...finalObjs];
        return this;
    }

    groupBy(...columns) {
        if (columns.length == 1 && Array.isArray(columns[0])) {
            columns = columns[0];
        }
        if (columns.some(el => typeof el !== 'string')) throw new TypeError('columns must contain only strings.');
        this.groupby = [...this.groupby, ...columns];
        return this;
    }

    limit(number) {
        if (this.limitn) throw new SyntaxError('limit alread registered.');
        if (!number || typeof number !== 'number' && (typeof number !== 'string' || Number.isNaN(Number(number)))) throw new TypeError('number deve representar um numero válido.');
        if (Number(number) < 0) throw new RangeError('number can not be negative.');
        this.limitn = Number(number);
        return this;
    }

    offSet(number) {
        if (this.offset) throw new SyntaxError('offSet alread registered.');
        if (!number || typeof number !== 'number' && (typeof number !== 'string' || Number.isNaN(Number(number)))) throw new TypeError('number deve representar um numero válido.');
        if (Number(number) < 0) throw new RangeError('number can not be negative.');
        this.offset = Number(number);
        return this;
    }

    toInstruction() {
        if (!this.action || !this.selectColumns) throw new SyntaxError('must define action before. (.select, .update, .insert or .delete).');
        if (this.have && !this.groupby) throw new SyntaxError('must groupBy action before. (.groupBy).');
        if (this.joins && this.fromTables.length > 1) throw new SyntaxError('joins only allows ONE TABLE.');
        let templateCount = 0;
        if (this.action === 'SELECT') {
            const select = (this.dist ? 'SELECT ' : 'SELECT DISTINCT') + ' ';
            const selectColumns = this.selectColumns.reduce((inc, el, i) => {
                const subStr = el.alias ? `${el.table} AS ${el.alias}` : el.table;
                if (i < this.selectColumns.length - 1) {
                    return inc + subStr + ', ';
                }
                else return inc + subStr;
            }, '') + ' ';

            let from = '';
            if (this.fromTables) {
                const tables = this.fromTables;
                from = 'FROM ' + tables.reduce((inc, el, i) => {
                    const subStr = `${el.alias ? el.table + ' AS ' + el.alias : el.table}`;

                    return inc + subStr + ' ';
                }, '');
            }

            const joins = this.joins.reduce((inc, el, i) => {
                //IF NATURAL-JOIN value1, value2 and operator are undefined
                const subStr = `${el.type} ${el.alias ? el.table + ' AS ' + el.alias : el.table} ${el.type !== QueryBuilder.joinTypes.naturalJoin ? 'ON' : ''} ${el.value1 || ''} ${el.operator || ''} ${el.value2 || ''}`;
                return inc + subStr + ' '
            }, '');

            let templateCount = 1;
            const valuesArray = [];
            let where = '';
            if (this.wheres.length > 0)
                where = 'WHERE ' + this.wheres.reduce((inc, el, i) => {
                    let strWhere;
                    if (i == 0) {
                        strWhere = `${el.column} ${el.operator} $${templateCount}`;
                    }
                    else {
                        strWhere = `${el.logicalOperator} ${el.column} ${el.operator} $${templateCount}`;
                    }
                    templateCount++;
                    valuesArray.push(el.value);
                    return inc + strWhere + ' ';

                }, '');

            let groupBy = '';
            if (this.groupby.length > 0) {
                groupBy = 'GROUP BY ' + this.groupby.join(', ') + ' ';
            }

            let having = '';
            if (this.have.length > 0) {
    
                having = 'HAVING ' + this.have.reduce((inc, el, i) => {
                    let strHaving;
                    if (i == 0) {
                        strHaving = `${el.column} ${el.operator} $${templateCount}`;
                    }
                    else {
                        strHaving = `${el.logicalOperator} ${el.column} ${el.operator} $${templateCount}`;
                    }
                    templateCount++;
                    valuesArray.push(el.value);
                    return inc + strHaving + ' ';
                },'');
            }

            let orderBy = ''
            if(this.orderby.length > 0) {
                orderBy = 'ORDER BY ' + this.orderby.map((el) => {
                    return `${el.column} ${el.order}`;
                }).join(', ') + ' ';
            }

            let limit = ''
            if(this.limitn){
                limit = 'LIMIT ' + this.limitn + ' ';
            }

            let offSet = ''
            if(this.offset){
                offSet = 'OFFSET ' + this.offset;
            }

            return select + selectColumns + from + joins + where + groupBy + having + orderBy + limit + offSet;

        }
    }
}

module.exports = QueryBuilder;