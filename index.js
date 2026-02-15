const {CaseClause:cc, Query:qb, CaseClause, Column} = require('./lib/index');
module.exports = qb;

const y = new CaseClause();
y.when('x','=','y', 2).when('u','z',4)
const x = new qb().select(y).whereIn('2', ['1','2']).orWhereIn('k', ['1' ,'2' , qb.column('a.b')]).where(1,2).join('z','k', '=', y).from('k');

console.log(x.toInstruction());

/*
 OUTPUT: 
 {
  template: 'DELETE FROM "a" WHERE ("x" = "z" AND ("x" = $1 OR "x" = "r"))',
  values: [ 2 ]
}
*/