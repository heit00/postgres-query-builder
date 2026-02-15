const { T } = require('./types');

//FILE CHANGE PATH 
//1<

const IR = {
    subQuery: 'SUBQUERY',
    statement: 'STATEMENT',
    cte: 'CTE'
}

function isCompilable(target) {
    if (typeof target !== 'object' || !target) return false;
    return (typeof target.toInstruction === 'function');
}

//uncommon types
function isQuery(target) {
    return target?.[Symbol.toStringTag] === 'Query' && isCompilable(target);
}

function isCase(target) {
    return target?.[Symbol.toStringTag] === 'CaseClause' && isCompilable(target);
}
//

class QueryGrammar {

    static relationalOperators = new Set(['>', '<', '=', '!=', '<=', '>=', '<>', 'IS', 'IS NOT']);

    static clauses = {
        distinct: 'DISTINCT',
        offSet: 'OFFSET',
        limit: 'LIMIT',
        from: 'FROM',
        where: 'WHERE',
        groupBy: 'GROUP BY',
        orderBy: 'ORDER BY',
        join: 'JOIN',
        having: 'HAVING',
        between: 'BETWEEN',
        with: 'WITH',
        returning: 'RETURNING',
        exists: 'EXISTS',
        using: 'USING',
        when: 'WHEN',
        end: 'END',
        then: 'THEN',
        case: 'CASE',
        else: 'ELSE'
    }

    static actions = {
        select: 'SELECT',
        insert: 'INSERT',
        delete: 'DELETE',
        update: 'UPDATE'
    }

    static joinTypes = {
        innerJoin: 'INNER JOIN',
        leftJoin: 'LEFT JOIN',
        rightJoin: 'RIGHT JOIN',
        fullOuterJoin: 'FULL OUTER JOIN',
        crossJoin: 'CROSS JOIN',
    };

    static logicalOperator = {
        or: 'OR',
        and: 'AND',
    };

    static orderByTypes = {
        asc: 'ASC',
        desc: 'DESC'
    };

    static extra = {
        in: 'IN',
        notIn: 'NOT IN'
    }
}
//>
//

//INTERNAL

class InExpression {
    #left;
    #right;
    #type;
    #isNot;

    #normalize(val, side = 'left') {

        if (val instanceof Column || val instanceof Bind || isCase(val) || isQuery(val)) {
            if (typeof val.hasAlias === 'function' && val.hasAlias()) throw new QuerySyntaxError('can not has alias in expressions.');

            return val;
        }

        if(T.v.filledString.func(val)) return side === 'left' ? new Column(val) : new Bind(val);

        else return new Bind(val)
    }

    constructor(left, right, type = QueryGrammar.logicalOperator.and, isNot = false) {
        if (typeof isNot !== 'boolean') throw new TypeError('isNot must be a boolean.');
        if (typeof type === 'string') type = type.toUpperCase();
        if (!Object.values(QueryGrammar.logicalOperator).includes(type)) throw new TypeError(`type must be in: (${Object.values(QueryGrammar.logicalOperator).join(', ')})`);

        if (Array.isArray(right)) {
            this.#right = right.map(el => {
                if(isQuery(el)) throw new QuerySyntaxError('subquery is not alowed in values array');
                return this.#normalize(el, 'right');
            });
        }
        else if (isQuery(right)) {
            this.#right = right;
        }
        else throw new TypeError('right must be an subquery or an array of values.');

        this.#type = type;
        this.#left = this.#normalize(left);
        this.#isNot = isNot;
    }

    toInstruction(hiddenType = false, count) {
        if (typeof hiddenType !== 'boolean') throw new TypeError('hiddenType must be a boolean.');

        const type = hiddenType ? '' : `${this.#type} `;
        const left = this.#left.toInstruction(count, IR.cte);

        const length = this.#right.length;
        const verb = this.#isNot ? QueryGrammar.extra.notIn : QueryGrammar.extra.in;

        if (Array.isArray(this.#right)) {
            const templateInExpression = this.#right.map(el => el.toInstruction(count)).join(', ');
            return `${type}${left} ${verb} (${templateInExpression})`;
        }
        else {
            const templateInExpression = this.#right.toInstruction(count, IR.cte);
            return `${type}${left} ${verb} ${templateInExpression}`;
        }
    }
}

class Expression {

    #type;
    #left;
    #operator;
    #right;

    #normalize(val, side = 'left') {

        if (val instanceof Column || val instanceof Bind || isCase(val) || isQuery(val)) {
            if (typeof val.hasAlias === 'function' && val.hasAlias()) throw new QuerySyntaxError('can not has alias in expressions.');

            return val;
        }

        if (T.v.filledString.func(val)) {
            return side === 'left' ? new Column(val) : new Bind(val);
        }

        else return new Bind(val);
    }

    constructor(left, operator, right, type = 'AND') {
        if (typeof type === 'string') type = type.toUpperCase();

        if (!QueryGrammar.relationalOperators.has(operator)) throw new TypeError(`operator must be in: (${[...QueryGrammar.relationalOperators].join(', ')})`);

        if (!Object.values(QueryGrammar.logicalOperator).includes(type)) throw new TypeError(`type must be in: (${Object.values(QueryGrammar.logicalOperator).join(', ')})`);

        this.#left = this.#normalize(left);
        this.#right = this.#normalize(right, 'right');

        this.#type = type;
        this.#operator = operator;

        //if(right === null && (operator !== 'IS' && operator !== 'IS NOT' || !(this.#left instanceof Column))) throw new QuerySyntaxError('conflict in null expression.');
    }

    toInstruction(hiddenType = false, count) {
        if (typeof hiddenType !== 'boolean') throw new TypeError('hiddenType must be a boolean.');
        let preString;
        const leftInst = this.#left.toInstruction(count, IR.subQuery);
        const rightInst = this.#right.toInstruction(count, IR.subQuery);
        if (!hiddenType) preString = [this.#type, leftInst, this.#operator, rightInst];
        else preString = [leftInst, this.#operator, rightInst];
        return preString.join(' ');
    }

}

class Bind {
    #value;

    constructor(value) {
        if (value === undefined) throw new TypeError('value can not be undefined.');
        this.#value = value;
    }

    //PLACE CHANGE
    toInstruction(count) {
        if (!(count instanceof TemplateCount)) throw new TypeError('Bind count is required for compilation.');
        return `$${count.idx(this.#value)}`;
    }
}

class TemplateCount {
    #idx = 0;
    #values = [];

    idx(value) {
        this.#values.push(value);
        return ++this.#idx;
    }

    getLiterals() {
        return this.#values;
    }
}

class BetWeen {
    #type;
    #left;
    #min;
    #max;


    #normalize(val) {

        if (
            isCase(val) ||
            val instanceof Bind ||
            val instanceof Column ||
            isQuery(val)
        ) {
            if (typeof val.hasAlias === 'function') if (val.hasAlias()) throw new QuerySyntaxError('can not has alias in between expression.');
            return val;
        }
        return new Bind(val);
    }

    constructor(left, [min, max] = [], type) {

        if (!Object.values(QueryGrammar.logicalOperator).includes(type)) throw new QuerySyntaxError(`type must be in: (${Object.values(QueryGrammar.logicalOperator).join(', ')})`);
        if (T.v.filledString(left)) left = new Column(left);
        left = this.#normalize(left);
        //else if (!(left instanceof Column)) throw new TypeError('left must be a column or a string.');
        //if(left.hasAlias()) throw new QuerySyntaxError('columns can not has alias in between expression.');

        this.#type = type;
        this.#left = left;
        this.#min = this.#normalize(min);
        this.#max = this.#normalize(max);
    }

    toInstruction(first, count) {
        if (typeof first !== 'boolean') throw new TypeError('first must be a boolean.');
        const operator = !first ? `${this.#type} ` : '';
        const verb = QueryGrammar.clauses.between;
        const and = QueryGrammar.logicalOperator.and;

        const finalStr = `${operator}${this.#left.toInstruction(count, IR.cte)} ${verb} ${this.#min.toInstruction(count, IR.cte)} ${and} ${this.#max.toInstruction(count, IR.cte)}`;
        return finalStr;
    }
}

class With {
    #query;
    #alias;

    constructor(query, alias) {
        if (!(isQuery(query))) throw new TypeError('query must be a instance of Query class.');
        if (!T.v.filledString.func(alias)) throw new TypeError('alias must be a string.');
        this.#query = query;
        this.#alias = alias;
    }

    toInstruction(count) {
        const alias = this.#alias;
        const subQuery = this.#query.toInstruction(count, IR.cte);
        return `${alias} AS ${subQuery}`;
    }
}

class Exists {
    #type
    #query;

    constructor(query, type) {
        if (!Object.values(QueryGrammar.logicalOperator).includes(type)) throw new QuerySyntaxError(`type must be in: (${Object.values(QueryGrammar.logicalOperator).join(', ')})`);
        if (!(isQuery(query))) throw new TypeError('query must be a instance of Query class.');
        this.#type = type;
        this.#query = query;
    }

    toInstruction(first, count) {
        if (typeof first !== 'boolean') throw new TypeError('first must be a boolean.');
        const operator = !first ? `${this.#type} ` : '';
        const exists = QueryGrammar.clauses.exists;
        const subQuery = this.#query.toInstruction(count, IR.cte);

        return `${operator}${exists} ${subQuery}`;
    }
}

class When {
    #where = new WhereClause();
    #value = undefined;

    when(left, operator, right) {


        // WHY only WHEN method has this? RESP: because this is not an WhereGroup, is acess to other complex structures, so when can do that while orWhen just do simple group with OR logical
        // you cand use when withiout callback so the default is simple AND expression...

        if (T.v.callable.func(left)) {
            left(this.#where);
            return this;
        }

        this.#where.where(left, operator, right);
        return this;
    }

    //orWhen would be useless

    then(value) {
        if (this.#value !== undefined) throw new QuerySyntaxError('then clause already defined.');

        //USE DUCKING TYPE
        if (!(value instanceof Column) && !(isQuery(value)) && !(value instanceof Bind) && !(isCase(value))) value = new Bind(value);
        if (typeof value.hasAlias === 'function') if (value.hasAlias()) throw new QuerySyntaxError('can not has alias in "THEN" expression.');
        this.#value = value;

        return this;
    }

    /**
     * @param {TemplateCount} count 
     */
    toInstruction(count) {
        if (this.#value === undefined) throw new QuerySyntaxError('then clause not defined.');

        const whereString = this.#where.toInstruction(true, count);
        const when = QueryGrammar.clauses.when;
        const then = QueryGrammar.clauses.then;
        const value = this.#value.toInstruction(count, IR.cte);

        return `${when} ${whereString} ${then} ${value}`;
    }
}


class CaseClause {

    get [Symbol.toStringTag]() { return 'CaseClause'; }

    #when = [];
    #alias = undefined;
    #else = undefined;
    #initial = undefined;

    constructor(alias = undefined, initial = undefined) {
        if (initial !== undefined && !(initial instanceof Column) && !(initial instanceof Bind) && !isQuery(initial) && !isCase(initial)) initial = new Bind(initial);
        const operators = Object.values(QueryGrammar.logicalOperator)
        if (!T.v.filledString.func(alias) && alias !== undefined) throw new TypeError('alias must be a string.');
        this.#alias = alias;
        this.#initial = initial;
    }

    as(alias) {
        if (this.#alias !== undefined) throw new QuerySyntaxError('alias already defined.');
        if (!T.v.filledString.func(alias)) throw new TypeError('alias must be a string.');
        this.#alias = alias;
    }

    hasAlias() {
        return this.#alias !== undefined;
    }

    when(left, operator, right, result) {
        if(result === undefined){
            result = right;
            right = operator;
            operator = '=';
        }

        if (T.v.callable.func(left)) {
            result = operator;
            const whenObj = new When();
            whenObj.when(left);
            whenObj.then(result);
            this.#when.push(whenObj);
            return this;
        }
        else {
            const whenObj = new When();
            whenObj.when(left, operator, right);
            whenObj.then(result);
            this.#when.push(whenObj);
            return this;
        }
    }

    else(value) {
        if (this.#else !== undefined) throw new QuerySyntaxError('else clause already defined.');

        if (!(value instanceof Column) && !(isQuery(value)) && !(value instanceof Bind) && !(isCase(value))) value = new Bind(value);
        this.#else = value;

        return this;
    }

    toInstruction(count) {

        const initial = this.#initial !== undefined ? ` ${this.#initial.toInstruction(count, IR.cte)}` : '';
        const caseVerb = QueryGrammar.clauses.case;
        const templateString = this.#when.map(el => el.toInstruction(count)).join(' ');
        const endVerb = QueryGrammar.clauses.end;
        const alias = this.#alias !== undefined ? ` AS ${this.#alias}` : '';
        const elseVerb = QueryGrammar.clauses.else;
        const elseValue = this.#else !== undefined ? this.#else.toInstruction(count, IR.cte) : undefined;
        const elseStatement = this.#else !== undefined ? ` ${elseVerb} ${elseValue}` : '';

        return `(${caseVerb}${initial} ${templateString}${elseStatement} ${endVerb})${alias}`;
    }
}

class WithClause {
    #withArray = [];

    add(w) {
        if (!(w instanceof With)) throw new TypeError('with must be an With class instance.');
        this.#withArray.push(w);

        return this;
    }

    isEmpty() {
        return this.#withArray.length === 0;
    }

    remove(idx) {
        if (!Number.isInteger(idx) || idx < 0) throw new TypeError('idx must be a valid integer >=0.');
        if (idx > this.#withArray.length - 1) throw new RangeError('idx not in range of array.');
        else this.#withArray.splice(idx, 1);

        return this;
    }

    toInstruction(count) {
        if (this.#withArray.length === 0) throw new QuerySyntaxError('array of with is empty.');
        const withStr = this.#withArray.map(el => el.toInstruction(count)).join(', ');
        return withStr;
    }
}

class WhereClause {

    #type;
    #expressions = [];

    constructor(type = QueryGrammar.logicalOperator.and) {
        if (typeof type === 'string') type = type.toUpperCase();
        if (!Object.values(QueryGrammar.logicalOperator).includes(type)) throw new TypeError(`type must be in: (${Object.values(QueryGrammar.logicalOperator).join(', ')})`);
        this.#type = type;
    }

    isEmpty() {
        return this.#expressions.length <= 0;
    }

    //
    where(left, operator, right) {
        return this.#add(left, operator, right, QueryGrammar.logicalOperator.and);
    }
    //
    orWhere(left, operator, right) {
        return this.#add(left, operator, right, QueryGrammar.logicalOperator.or);
    }

    #add(left, operator, right, type) {

        if (right === undefined) {
            right = operator;
            operator = '=';
        }

        if (!(right instanceof Column) && !(right instanceof Bind) && !(isQuery(right)) && !(isCase(right))) right = new Bind(right); //
        //IF is not a object o string the error will exploood in new Expression();

        const exp = new Expression(left, operator, right, type);
        this.#expressions.push(exp);
        return this;

    }

    whereIn(left, right) {
        return this.#addIn(left, right, false, QueryGrammar.logicalOperator.and);
    }

    orWhereIn(left, right) {
        return this.#addIn(left, right, false, QueryGrammar.logicalOperator.or);
    }

    whereNotIn(left, right) {
        return this.#addIn(left, right, true, QueryGrammar.logicalOperator.and);
    }

    orWhereNotIn(left, right) {
        return this.#addIn(left, right, true, QueryGrammar.logicalOperator.or);
    }

    #addIn(left, right, isNot = false, type) {
        const exp = new InExpression(left, right, type, isNot);
        this.#expressions.push(exp);
        return this;
    }

    //
    whereBetween(left, [min, max]) {
        return this.#addBetween(left, [min, max], QueryGrammar.logicalOperator.and);
    }

    //
    orWhereBetween(left, [min, max]) {
        return this.#addBetween(left, [min, max], QueryGrammar.logicalOperator.or);
    }

    #addBetween(left, [min, max] = [], type) {
        const betw = new BetWeen(left, [min, max], type);
        this.#expressions.push(betw);
        return this;
    }

    //
    whereExists(query) {
        return this.#addExists(query, QueryGrammar.logicalOperator.and);
    }

    //
    orWhereExists(query) {
        return this.#addExists(query, QueryGrammar.logicalOperator.or);
    }

    #addExists(query, type) {
        const exis = new Exists(query, type);
        this.#expressions.push(exis);
        return this;
    }

    whereGroup(callBack) {
        return this.#addGroup(callBack, QueryGrammar.logicalOperator.and);
    }

    orWhereGroup(callBack) {
        return this.#addGroup(callBack, QueryGrammar.logicalOperator.or);
    }

    #addGroup(callBack, type) {
        if (!T.v.callable.func(callBack)) throw new TypeError('callBack ust be calleable.');
        const subWhereClause = new WhereClause(type);
        callBack(subWhereClause);
        this.#expressions.push(subWhereClause);
        return this;
    }

    toInstruction(hiddenType = false, countObj) {
        if (this.#expressions.length === 0) throw new QuerySyntaxError('conditions not initialized.');

        if (typeof hiddenType !== 'boolean') throw new TypeError('hiddenType must be a boolean.');
        const str = `${!hiddenType ? this.#type + ' ' : ''}(${this.#expressions.map((el, i) => {
            if (i === 0)
                return el.toInstruction(true, countObj);
            else return el.toInstruction(false, countObj);
        }).join(' ')})`;

        return str;
    }
}

class JoinClause {
    #table;
    #type;
    #whereClause = new WhereClause();

    constructor(table, type) {
        this.#table = ((table instanceof Table) || (isQuery(table))) ? table : new Table(table);
        if (!Object.values(QueryGrammar.joinTypes).includes(type)) throw new QuerySyntaxError(`type must be in: (${Object.values(QueryGrammar.joinTypes).join(', ')})`);
        this.#type = type;
    }

    get whereClause() {
        return this.#whereClause;
    }

    on(left, operator, right) {
        return this.#on(left, operator, right, QueryGrammar.logicalOperator.and);
    }

    onOr(left, operator, right) {
        return this.#on(left, operator, right, QueryGrammar.logicalOperator.or);
    }

    #on(left, operator, right, type) {

        if(T.v.callable.func(left)){
            left(this.#whereClause);
            return this;
        }

        if (right === undefined) {
            right = operator;
            operator = '=';
        }
        if (!(right instanceof Column) && !(right instanceof Bind) && !(isCase(right)) && !(isQuery(right)) && T.v.filledString.func(right)) right = new Column(right);
        if (type === QueryGrammar.logicalOperator.and) {
            this.#whereClause.where(left, operator, right);
        }
        else if (type === QueryGrammar.logicalOperator.or) {
            this.#whereClause.orWhere(left, operator, right);
        }
        else throw new TypeError(`type must be in: (${Object.values(QueryGrammar.logicalOperator)})`);
        return this;
    }

    onGroup(callBack) {
        this.#whereClause.whereGroup(callBack);
        return this;
    }

    onGroupOr(callBack) {
        this.#whereClause.orWhereGroup(callBack);
        return this;
    }

    //PLACE CHANGE
    toInstruction(countObj) {
        const isCross = this.#type === QueryGrammar.joinTypes.crossJoin;

        if (this.#whereClause.isEmpty() && !isCross) throw new QuerySyntaxError('on clause not found.');
        if (!this.#whereClause.isEmpty() && isCross) throw new QuerySyntaxError('on clause in cross join is bad syntax.');

        const str = [];
        str.push(this.#type);
        str.push(this.#table.toInstruction(countObj, IR.subQuery));

        if (!isCross) {
            str.push('ON');
            str.push(this.#whereClause.toInstruction(true, countObj));
        }

        return str.join(' ');
    }
    //
}

class Column {
    #name;
    #alias;
    #table

    constructor(name, alias = undefined) {

        if (!T.v.filledString.func(alias) && alias !== undefined) throw new TypeError('alias must be undefined or a string.');
        if (typeof name !== 'string' || name.trim().length === 0) throw new QuerySyntaxError('column must be an string.');
        const parts = name.split('.');
        if (parts.length > 2) throw new QuerySyntaxError('namespace table must have 1 dot.');

        this.#table = parts.length > 1 ? parts[0] : undefined;
        this.#name = parts.length > 1 ? parts[1] : parts[0];
        this.#alias = alias;

    }

    hasAlias() {
        return this.#alias !== undefined;
    }

    get alias() {
        return this.#alias;
    }

    get name() {
        return this.#name;
    }

    get table() {
        return this.#table;
    }

    //PLACE CHANGE
    toInstruction() {

        const str1 = `"${this.#name}"`;
        const str2 = this.#table ? `"${this.#table}"` : '';
        const str3 = this.#alias ? ` AS "${this.#alias}"` : '';

        const finalStr = `${str2.trim().length > 0 ? str2 + '.' : ''}${str1}${str3}`;
        return finalStr;
    }
}

class OrderBy {

    #column;
    #type;

    constructor(column, type = QueryGrammar.orderByTypes.asc) {
        if (typeof type === 'string') type = type.toUpperCase();
        if (T.v.filledString.func(column)) column = new Column(column);
        else if (!(column instanceof Column) && !(isCase(column)) && !(isQuery(column))) throw new TypeError('column must be a string, a instance of Column class or a caseClause instance.');

        if (typeof column.hasAlias === 'function' && column.hasAlias()) {
            throw new QuerySyntaxError('Expression cannot have an alias in order by clause.');
        }


        if (!Object.values(QueryGrammar.orderByTypes).includes(type)) throw new TypeError(`type must be in: (${Object.values(QueryGrammar.orderByTypes).join(', ')})`);

        this.#column = column;
        this.#type = type;
    }

    get column() {
        return this.#column;
    }

    get type() {
        return this.#type;
    }

    toInstruction(count) {
        const columnStr = this.#column.toInstruction(count, IR.cte);
        const type = this.#type;
        const finalStr = `${columnStr} ${type}`;

        return finalStr;
    }

}

class Table {
    #name;
    #alias
    constructor(name, alias = undefined) {
        if (!T.v.filledString.func(alias) && alias !== undefined) throw new TypeError('alias must be undefined or a string.');
        if (typeof name !== 'string' || name.trim().length === 0) throw new QuerySyntaxError('column must be an string.');
        this.#name = name;
        this.#alias = alias;//
    }

    hasAlias() {
        this.#alias !== undefined;
    }

    toInstruction() {
        const str1 = `"${this.#name}"`;
        const str3 = this.#alias ? ` AS "${this.#alias}"` : '';

        const finalStr = `${str1}${str3}`;
        return finalStr;
    }
}

class QuerySyntaxError extends Error {
    constructor(message) {
        super(message);
        this.name = 'QuerySyntaxError'
    }
}

class Query {

    get [Symbol.toStringTag]() { return 'Query'; }

    #using = [];
    #returningColumns = [];
    #with = new WithClause();
    #alias = undefined;
    #limit = undefined;
    #offset = undefined;
    #distinct = undefined;
    #fromTables = [];
    #selectColumns = [];
    #action = undefined;
    #orderBy = [];
    #join = [];
    #where = new WhereClause();
    #groupBy = [];
    #having = new WhereClause();

    static column(name, alias) {
        return new Column(name, alias);
    }

    static bind(value) {
        return new Bind(value);
    }

    static table(name) {
        return new Table(name);
    }

    hasAlias() {
        return this.#alias !== undefined;
    }

    select(...columns) {
        if (!this.#action || this.#action === QueryGrammar.actions.select) this.#action = QueryGrammar.actions.select;
        else throw new QuerySyntaxError(`a action is already registered: (${this.#action})`);

        if (columns.length === 1 && Array.isArray(columns[0])) columns = columns[0];

        this.#selectColumns = [...this.#selectColumns, ...columns.map(el => {
            if (T.v.filledString.func(el)) return new Column(el);
            if ((el instanceof Column) || isCase(el) || (el instanceof Bind) || isQuery(el)) return el;
            throw new TypeError('columns must contains valid column representations.');
        })];

        return this;
    }

    from(...tables) {
        if (tables.length === 1 && Array.isArray(tables[0])) tables = tables[0];

        this.#fromTables = [...this.#fromTables, ...tables.map(el => {
            if (T.v.filledString.func(el)) return new Table(el);
            if (el instanceof Table) return el;
            if (isQuery(el)) return el;
            throw new TypeError('tables must contains valid table representation.');
        })];

        return this;
    }

    join(table, left, operator, right) {
        return this.#addJoin(QueryGrammar.joinTypes.innerJoin, table, left, operator, right);
    }

    leftJoin(table, left, operator, right) {
        return this.#addJoin(QueryGrammar.joinTypes.leftJoin, table, left, operator, right);
    }

    rightJoin(table, left, operator, right) {
        return this.#addJoin(QueryGrammar.joinTypes.rightJoin, table, left, operator, right);
    }

    crossJoin(table) {
        return this.#addJoin(QueryGrammar.joinTypes.crossJoin, table);
    }

    fullJoin(table, left, operator, right) {
        return this.#addJoin(QueryGrammar.joinTypes.fullOuterJoin, table, left, operator, right);
    }

    #addJoin(type, table, left, operator, right) {
        if (right === undefined && typeof left === 'string') {
            right = operator;
            operator = '=';
        }

        if (T.v.filledString.func(table)) table = new Table(table);
        else if (!(table instanceof Table) && !(isQuery(table))) throw new TypeError('table must be a string, instance of Table class. or instance of Query');
        const joinClause = new JoinClause(table, type);

        if (type === QueryGrammar.joinTypes.crossJoin) {
            //Dont need config
        }
        //else if ((T.v.filledString.func(left))) {
            //if (!QueryGrammar.relationalOperators.has(operator)) throw new QuerySyntaxError(`operator must be in: (${[...QueryGrammar.relationalOperators].join(', ')})`);
           // if (!T.v.filledString.func(right)) throw new TypeError('right must be a string.');
            //joinClause.on(left, operator, right);
        //}
        else if (T.v.callable.func(left)) {
            //operator and right are ignored
            const func = left;
            func(joinClause);
        }
        else {
            //if (!QueryGrammar.relationalOperators.has(operator)) throw new QuerySyntaxError(`operator must be in: (${[...QueryGrammar.relationalOperators].join(', ')})`);
            //if (!T.v.filledString.func(right)) throw new TypeError('right must be a string.');
            joinClause.on(left, operator, right);
        }

        this.#join.push(joinClause);
        return this;
    }

    //
    where(left, operator, right) {
        this.#where.where(left, operator, right);
        return this;
    }

    //
    whereBetween(left, [min, max]) {
        this.#where.whereBetween(left, [min, max]);
        return this;
    }

    //
    orWhereBetween(left, [min, max]) {
        this.#where.orWhereBetween(left, [min, max]);
        return this;
    }

    //
    whereExists(query) {
        this.#where.whereExists(query);
        return this;
    }

    //
    orWhereExists(query) {
        this.#where.orWhereExists(query);
        return this;
    }

    whereIn(left, right) {
        this.#where.whereIn(left, right);
        return this;
    }

    orWhereIn(left, right) {
        this.#where.orWhereIn(left, right);
        return this;
    }

    whereNotIn(left, right) {
        this.#where.whereNotIn(left, right);
        return this;
    }

    orWhereNotIn(left, right) {
        this.#where.orWhereNotIn(left, right);
        return this;
    }

    //
    orWhere(left, operator, right) {
        this.#where.orWhere(left, operator, right);
        return this;
    }

    whereGroup(callBack) {
        this.#where.whereGroup(callBack);
        return this;
    }

    orWhereGroup(callBack) {
        this.#where.orWhereGroup(callBack);
        return this;
    }

    having(left, operator, right) {
        return this.#addHaving(left, operator, right, QueryGrammar.logicalOperator.and);
    }

    havingGroup(callBack) {
        return this.#addHavingGroup(callBack, QueryGrammar.logicalOperator.and);
    }

    orHavingGroup(callBack) {
        return this.#addHavingGroup(callBack, QueryGrammar.logicalOperator.or);
    }

    orHaving(left, operator, right) {
        return this.#addHaving(left, operator, right, QueryGrammar.logicalOperator.or);
    }

    delete() {
        if (!this.#action) this.#action = QueryGrammar.actions.delete;
        else throw new QuerySyntaxError(`a action is already registered: (${this.#action})`);

        return this;
    }

    using(...table) {
        if (table.length === 1 && Array.isArray(table[0])) table = table[0];
        this.#using = [... this.#using, ...table.map(el => {
            if (T.v.filledString.func(el)) return new Table(el);
            else if (el instanceof Table) return el;
            else if (isQuery(el)) return el;
            else throw new TypeError('table elements must be an string or table class instance.');
        })];
        return this;
    }

    #addHaving(left, operator, right, type) {
        if (right === undefined) {
            right = operator;
            operator = '=';
        }
        if (type === QueryGrammar.logicalOperator.and) {
            this.#having.add(left, operator, right);
        }
        else if (type === QueryGrammar.logicalOperator.or) {
            this.#having.addOr(left, operator, right);
        }
        else throw new TypeError(`type must be in: (${Object.values(QueryGrammar.logicalOperator).join(', ')})`);
        return this;
    }

    #addHavingGroup(callBack, type) {
        if (!T.v.callable.func(callBack)) throw new TypeError('callback must be calleable.');
        if (type === QueryGrammar.logicalOperator.and)
            this.#having.addGroup(callBack);
        else if (type === QueryGrammar.logicalOperator.or)
            this.#having.addGroupOr(callBack);
        else throw new TypeError(`type must be in: (${Object.values(QueryGrammar.logicalOperator).join(', ')})`);
        return this;
    }

    distinct() {
        this.#distinct = QueryGrammar.clauses.distinct;
        return this;
    }

    orderBy(...columns) {
        if (columns.length === 1 && Array.isArray(columns[0])) columns = columns[0];
        columns.forEach(el => {
            if (el instanceof OrderBy) {
                this.#orderBy.push(el);
            } else if (T.v.filledString.func(el)) this.#orderBy.push(new OrderBy(el));
            else if ((el instanceof Column) || (isCase(el)) || (isQuery(el))) this.#orderBy.push(new OrderBy(el));
            else throw new TypeError('column must contain strings , Columns or OrderBy instances.');
        });
        return this;
    }

    groupBy(...columns) {
        if (columns.length === 1 && Array.isArray(columns[0])) columns = columns[0];
        columns.forEach(el => {
            if ((el instanceof Column) || (isCase(el))) {
                if (el.hasAlias()) throw new QuerySyntaxError('groupBy does not allow alias in columns or cases clauses.');
                this.#groupBy.push(el);
            } else if (T.v.filledString.func(el)) this.#groupBy.push(new Column(el));
            else throw new TypeError('column must contain strings , Columns instances or cases instances.');
        });
        return this;
    }

    limit(number) {
        if (!Number.isInteger(number) || number <= 0) throw new TypeError('number must be a integer > 0.');
        this.#limit = number;
        return this;
    }

    offset(number) {
        if (!Number.isInteger(number) || number < 0) throw new TypeError('number must be a integer > 0.');
        this.#offset = number;
        return this;
    }

    as(alias) {
        if (!T.v.filledString.func(alias)) throw new TypeError('alias must be an string.');
        this.#alias = alias;

        return this;
    }

    returning(...columns) {
        if (columns.length === 1 && Array.isArray(columns[0])) columns = columns[0];

        const returnColumns = columns.map(el => {
            if ((el instanceof Column) || (isCase(el)) || (isQuery(el))) return el;
            else if (T.v.filledString.func(el)) return new Column(el);
            else throw new TypeError('el must be a column instance a string or a case instance.');
        });

        this.#returningColumns = [...this.#returningColumns, ...returnColumns];
        return this;
    }

    with(qb, alias) {
        this.#with.add(new With(qb, alias));
        return this;
    }

    #buildWith(count) {
        if (this.#with.isEmpty()) return '';
        const verb = QueryGrammar.clauses.with;

        return `${verb} ${this.#with.toInstruction(count)}`;
    }

    //SELECT
    #buildSelect(count) {

        const distinct = this.#distinct ? QueryGrammar.clauses.distinct : undefined;

        let str1 = QueryGrammar.actions.select;
        if (distinct !== undefined) str1 = `${str1} ${QueryGrammar.clauses.distinct}`;

        const columns = this.#selectColumns.map(el => el.toInstruction(count, IR.subQuery)).join(', ');

        return [str1, columns.length === 0 ? '*' : columns].join(' ');
    }

    #buildJoin(count) {
        if (this.#join.length === 0) return '';
        if(this.#fromTables.length !== 1) throw new QuerySyntaxError('in join clauses, one FROM need to be used.');

        const joins = this.#join.map(el => el.toInstruction(count)).join(' ');
        return joins;
    }

    #buildWhere(count) {
        if (this.#where.isEmpty()) return '';
        return [QueryGrammar.clauses.where, this.#where.toInstruction(true, count)].join(' ');
    }

    #buildGroupBy(count) {
        if (this.#groupBy.length === 0) return '';
        const verb = QueryGrammar.clauses.groupBy;
        const groupByArray = this.#groupBy.map(el => el.toInstruction(count));
        return `${verb} ${groupByArray.join(', ')}`;
    }

    #buildHaving(count) {
        if (this.#having.isEmpty()) return '';
        if (this.#groupBy.length === 0) throw new QuerySyntaxError('having can not exist without group by');
        const whereHaving = this.#having.toInstruction(true, count);
        const verb = QueryGrammar.clauses.having;

        return `${verb} ${whereHaving}`;
    }

    #buildOrderBy(count) {
        if (this.#orderBy.length === 0) return '';

        const orderBy = this.#orderBy.map(el => el.toInstruction(count)).join(', ');
        const verb = QueryGrammar.clauses.orderBy;
        return `${verb} ${orderBy}`;
    }

    #buildLimit() {
        if (this.#limit === undefined) return '';
        if (this.#action !== QueryGrammar.actions.select) throw new QuerySyntaxError('limit must be only in select clause.');
        const verb = QueryGrammar.clauses.limit;

        return `${verb} ${this.#limit}`;
    }

    #buildOffset() {
        if (this.#offset === undefined) return '';
        if (this.#action !== QueryGrammar.actions.select) throw new QuerySyntaxError('offset must be only in select clause.');
        const verb = QueryGrammar.clauses.offSet;

        return `${verb} ${this.#offset}`;
    }

    #buildReturning(count) {
        if (this.#returningColumns.length === 0) return '';
        if (this.#action === QueryGrammar.actions.select) throw new QuerySyntaxError('returning in SQL is invalid.');
        const verb = QueryGrammar.clauses.returning;

        const column = this.#returningColumns.map(el => el.toInstruction(count, IR.subQuery)).join(', ');

        return `${verb} ${column}`;
    }

    //SELECT | DELETE
    #buildFrom(count) {
        if(this.#fromTables.length === 0) return '';
        
        //else if (this.#join.length > 0) throw new QuerySyntaxError('in join clause, all tables must be defined in .join method.');

        const verb = QueryGrammar.clauses.from;

        if (this.#action === QueryGrammar.actions.delete) {
            if (this.#fromTables.length > 1) throw new QuerySyntaxError('in delete actio, just 1 table can be defined.');
            const table = this.#fromTables[0];
            if (isQuery(table)) throw new QuerySyntaxError('subQuery is now allowed in delete clause.');


            return [verb, table.toInstruction()].join(' ');
        }

        else {
            const tables = this.#fromTables.map(el => el.toInstruction(count, IR.subQuery)).join(', ');
            return [verb, tables].join(' ');
        }

    }

    //DELETE
    #buildDelete() {
        return QueryGrammar.actions.delete
    }

    #buildUsing(count) {
        if (this.#action !== QueryGrammar.actions.delete && this.#action !== QueryGrammar.actions.update) throw new QuerySyntaxError('actions must be update or delete to use using clause.');
        if (this.#using.length === 0) return '';
        const verb = QueryGrammar.clauses.using;
        const usingTables = this.#using.map(el => el.toInstruction(count, IR.subQuery));

        return `${verb} ${usingTables.join(', ')}`;
    }

    //COMPILERS

    #compileSelect(count) {
        const steps = [
            this.#buildWith(count),
            this.#buildSelect(count),
            this.#buildFrom(count),
            this.#buildJoin(count),
            this.#buildWhere(count),
            this.#buildGroupBy(count),
            this.#buildHaving(count),
            this.#buildOrderBy(count),
            this.#buildLimit(),
            this.#buildOffset()
        ];

        const finalSteps = steps.filter(step => step && step.length > 0);
        return finalSteps.join(' ');
    }

    #compileDelete(count) {
        const steps = [
            this.#buildWith(count),
            this.#buildDelete(),
            this.#buildFrom(count),
            this.#buildUsing(count),
            this.#buildWhere(count),
            this.#buildReturning(count)
        ];

        const finalSteps = steps.filter(step => step && step.length > 0);
        return finalSteps.join(' ');
    }

    toInstruction(count, mode) {
        if (typeof mode === 'string') mode = mode.toUpperCase();
        if (mode && !Object.values(IR).includes(mode)) throw new QuerySyntaxError(`mode must be in: (${Object.values(IR).join(', ')})`);

        if (!mode) {
            return this.#toInstruction();
        }
        else if (mode === IR.subQuery) {
            if (this.#action !== QueryGrammar.actions.select) throw new QuerySyntaxError('in subStatement action must be select.');
            const alias = this.#alias ? ` AS ${this.#alias}` : '';
            const finalStr = `(${this.#toInstruction(count).template})${alias}`;
            return finalStr;
        }
        else if (mode === IR.cte) {
            if (this.#action !== QueryGrammar.actions.select) throw new QuerySyntaxError('in subStatement action must be select.');
            const finalStr = `(${this.#toInstruction(count).template})`;

            return finalStr;
        }
        else throw new QuerySyntaxError(`Unsupported render mode: ${mode}`);
    }

    #toInstruction(count) {
        if (!count)
            count = new TemplateCount();
        if (this.#action === QueryGrammar.actions.select) {
            const template = this.#compileSelect(count);
            const values = count.getLiterals();
            return { template, values };
        }
        else if (this.#action === QueryGrammar.actions.delete) {
            const template = this.#compileDelete(count);
            const values = count.getLiterals();
            return { template, values };
        }
        else throw new Error('sorry i did not created others still :D');
        //...
    }

}

module.exports = { Query, CaseClause, Column };

