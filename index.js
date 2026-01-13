const qb = require('./lib/index');

const q = new qb();
q.select('a','vb','c').from('x').join({config: {name: 'namealias'},value1: 'x',value2: 'y'}).where({column: 'x', operator: '=', value: 'z'}).having('x','IN','(1,2,3)').orderBy('x','y').limit('2').offSet('22');
console.log(q.toInstruction());
//output: SELECT DISTINCT a, vb, c FROM x INNER JOIN namealias AS name ON x = y




