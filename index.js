import {Bool, Field, Int32, Int64, Schema, Type} from "apache-arrow";
import {Utf8} from "apache-arrow/type";


class ColumnVector {
    /**
     * @return {Type}
     */
    getType() {
        throw "No!"
    }

    getValue(i) {
        throw "Nope"
    }

    get size() {
        return 0;
    }
}

class LiteralValueVector extends ColumnVector {
    arrowType;
    value;
    size;

    constructor(arrowType, value, size) {
        super();
        this.arrowType = arrowType;
        this.value = value;
        this.size = size;
    }

    getType() {
        return this.arrowType;
    }

    getValue(i) {
        if (i < 0 || i >= size) {
            throw "range"
        } else {
            return this.value
        }
    }
}

class RecordBatch {
    schema;
    fields;

    constructor(schema, fields) {
        this.schema = schema;
        this.fields = fields;
    }

    rowCount() {
        return this.fields[0].length;
    }

    columnCount() {
        return this.fields.length;
    }

    field(n) {
        return this.fields[n];
    }
}


class DataSource {
    get schema() {
    }

    /**
     * @param {string[]} projection
     * @return {RecordBatch[]}
     */
    scan(projection) {
    }
}

class CsvDataSource extends DataSource {
    constructor(path) {
        super();
    }
}


class LogicalPlan {
    /**
     * @return {LogicalPlan[]}
     */
    get children() {
        return []
    }
}

function format(plan, indent = 0) {
    console.log(' '.repeat(indent), plan.toString());
    for (let child of plan.children || []) {
        format(child, indent + 2);
    }
}

class LogicalExpr {
    /**
     * @param {LogicalPlan} input
     * @return {Field}
     */
    toField(input) {
    }

    toString() {
        throw 'LogicalExpr::toString'
    }
}


class Column extends LogicalExpr {
    constructor(name) {
        super();
        this.name = name;
    }

    toField(input) {
        return input.schema.find(x => x.name === this.name);
    }

    toString() {
        return `#${this.name}`;
    }
}

class LiteralString extends LogicalExpr {
    constructor(str) {
        super();
        this.str = str;
    }

    toField(input) {
        return new Field(this.str, Utf8);
    }

    toString() {
        return `'${this.str}'`;
    }
}

class BinaryExpr {
    constructor(name, op, l, r) {
        this.name = name;
        this.op = op;
        this.l = l;
        this.r = r;
    }

    toString() {
        return `${this.l} ${this.op} ${this.r}`;
    }
}


class BooleanBinaryExpr extends BinaryExpr {
    constructor(name, op, l, r) {
        super(name, op, l, r);
    }

    toField(input) {
        return new Field(this.name, Bool);
    }

    toString() {
        return super.toString();
    }
}

class EqExpr extends BooleanBinaryExpr {
    constructor(l, r) {
        super("eq", "=", l, r);
    }

    toString() {
        return super.toString();
    }
}

class NeqExpr extends BooleanBinaryExpr {
    constructor(l, r) {
        super("neq", "!=", l, r);
    }
}

class MathExpr extends LogicalExpr {
    name;
    op;
    l;
    r;

    constructor(name, op, l, r) {
        super();
        this.name = name;
        this.op = op;
        this.l = l;
        this.r = r;
    }

    toField(input) {
        return new Field(this.name, this.l.toField(input).dataType)
    }
}

class Mul extends MathExpr {
    constructor(l, r) {
        super("mul", "*", l, r);
    }
}

class Add extends MathExpr {
    constructor(l, r) {
        super("add", "+", l, r);
    }
}

// TODO: Sub
// TODO: Div
// TODO: Mod

class AggregateExpr {
    name;
    expr;

    constructor(name, expr) {
        this.name = name;
        this.expr = expr;
    }

    /**
     * @param input
     * @returns {Field<string | number>}
     */
    toField(input) {
        return new Field(this.name, this.expr.toField(input).dataType)
    }

    toString() {
        return `${this.name}(${this.expr})`;
    }
}

class Sum extends AggregateExpr {
    constructor(input) {
        super("SUM", input);
    }
}

class Count extends AggregateExpr {
    constructor(input) {
        super("COUNT", input);
    }

    toField(input) {
        return new Field("COUNT", Int32)
    }

    toString() {
        return `COUNT(${this.expr})`;
    }
}

// TODO: Min
// TODO: Max
// TODO: Avg

class Scan extends LogicalPlan {
    constructor(path, dataSource, projection = []) {
        super();
        this.path = path;
        this.dataSource = dataSource;
        this.projection = projection;
        this.schema = this.deriveSchema()
    }

    /**
     * @returns {Schema}
     */r

    deriveSchema() {
        let schema = this.dataSource.schema;
        if (this.projection.length === 0) {
            return schema;
        } else {
            return schema.select(this.projection);
        }
    }

    toString() {
        if (this.projection.length === 0) {
            return `Scan: ${this.path}; projection=None`
        } else {
            return `Scan: ${this.path}; projection=${this.projection}`
        }
    }
}

class Projection extends LogicalPlan {
    /**
     * @param {LogicalPlan} input
     * @param expr {LogicalExpr[]}
     */
    constructor(input, expr) {
        super();
        this.input = input;
        this.expr = expr;
    }

    schema() {
        return new Schema(this.expr.map(x => x.toField(this.input)));
    }

    /**
     * @return {LogicalPlan[]}
     */
    get children() {
        return [this.input]
    }

    toString() {
        let expressions = this.expr.map(x => x.toString()).join(", ");
        return `Projection: ${expressions}`
    }
}

// Selection aka Filter
class Selection extends LogicalPlan {
    /**
     * @param {LogicalPlan} input
     * @param {BooleanBinaryExpr} expr
     */
    constructor(input, expr) {
        super();
        this.input = input;
        this.expr = expr;
    }

    schema() {
        return this.input.schema;
    }

    /**
     * @returns {LogicalPlan[]}
     */
    get children() {
        return [this.input]
    }

    // get [Symbol.toStringTag]() {
    //     return `Projection: ${this.expr.map(x => x.toString()).join(", ")}`
    // }
    toString() {
        return `Filter: ${this.expr}`
    }
}

class Aggregate extends LogicalPlan {
    input;
    groupExpr;
    aggregateExpr;

    constructor(input, groupExpr, aggregateExpr) {
        super();
        this.input = input;
        this.groupExpr = groupExpr;
        this.aggregateExpr = aggregateExpr;
    }

    schema() {
        let groupExpr = this.groupExpr.map(x => x.toField(this.input));
        let aggregateExpr = this.aggregateExpr.map(x => x.toField(this.input));
        return new Schema(groupExpr + aggregateExpr);
    }

    get children() {
        return [this.input];
    }

    toString() {
        return `Aggregate: groupExpr=${this.groupExpr}, aggregateExpr=${this.aggregateExpr}`;
    }
}

// let csv = new CsvDataSource("employee.csv");
// let scan = new Scan("employee", csv);
// let filterExpr = new EqExpr(new Column("state"), new LiteralString("CO"));
// let selection = new Selection(scan, filterExpr);
// let projectionList = [
//     new Column("id"),
//     new Column("first_name"),
//     new Column("last_name"),
//     new Column("state"),
//     new Column("salary"),
// ];
// let plan = new Projection(selection, projectionList);
// format(plan)

class DataFrame {
    constructor(plan) {
        this.plan = plan;
    }

    project(expr) {
        return new DataFrame(new Projection(this.plan, expr));
    }

    filter(expr) {
        return new DataFrame(new Selection(this.plan, expr))
    }

    aggregate(groupBy, aggregateExpr) {
        return new DataFrame(new Aggregate(this.plan, groupBy, aggregateExpr));
    }

    get schema() {
        return this.plan.schema();
    }

    logicalPlan() {
        return this.plan;
    }

    toString() {
        return `DataFrame(${this.plan})`;
    }
}

class ExecutionContext {
    csv(filename) {
        return new DataFrame(new Scan(filename, new CsvDataSource(filename)));
    }
}

let col = name => new Column(name);
let eq = (x, y) => new EqExpr(x, y);
let str = s => new LiteralString(s);

let ctx = new ExecutionContext()
let df = ctx.csv("employee.csv")
    .filter(eq(col("state"), str("CO")))
    .project([
        col("id"),
        col("first_name"),
        col("last_name"),
        col("state"),
        col("salary"),
    ]);
format(df.logicalPlan());
console.log(df)

class PhysicalPlan {
    /**
     * @return {Schema}
     */
    schema() {
    }

    /**
     * @return {RecordBatch}
     */
    * execute() {
    }

    children() {
    }
}

class Expression {
    /**
     * @return {ColumnVector}
     * @param {RecordBatch} input
     */
    evaluate(input) {
        throw 'PhysicalExpression::evaluate'
    }
}

class ColumnExpression extends Expression {
    /**
     *
     * @param {Number} i we don't want to do lookups all the time in the physical plan, use index directly
     */
    constructor(i) {
        super();
        this.i = i;
    }

    evaluate(input) {
        return input.field[this.i];
    }

    toString() {
        return `#${this.i}`;
    }
}

// class LiteralValueVector extends Expression {
//     arrowType;
//     value;
//     size;
//
//     constructor(arrowType, value, size) {
//         super();
//         this.arrowType = arrowType;
//         this.value = value;
//         this.size = size;
//     }
//
//     getType() {
//         return this.arrowType;
//     }
//
//     getValue(i) {
//         if (i < 0 || i >= this.size) {
//             throw 'out of range'
//         } else {
//             return this.value;
//         }
//     }
//
//     get size() {
//         return this.size;
//     }
// }

class LiteralLongExpression extends Expression {
    value;

    constructor(value) {
        super();
        this.value = value;
    }

    evaluate(input) {
        return new LiteralValueVector(Int64, this.value, input.rowCount());
    }
}

class LiteralDoubleExperssion extends Expression {
    value;

    constructor(value) {
        super();
        this.value = value;
    }

    evaluate(input) {
        return new LiteralValueVector(Type.Float64, this.value, input.rowCount());
    }
}

class LiteralStringExpression extends Expression {
    value;

    constructor(value) {
        super();
        this.value = value;
    }

    evaluate(input) {
        return new LiteralValueVector(Type.Utf8, this.value.toInt8Array(), input.rowCount());
    }
}

class BinaryExpression extends Expression {
    l;
    r;

    constructor(l, r) {
        super();
        this.l = l;
        this.r = r;
    }

    evaluate(input) {
        let ll = this.l.evaluate(input);
        let rr = this.r.evaluate(input);
        if (ll.size !== rr.size) {
            throw 'BinaryExpression::evaluate, l and r size differ'
        }
        if (ll.getType() !== rr.getType()) {
            throw `BinaryExpresssion::evalute, type mismatch ${ll.getType()} !== ${rr.getType()}`;
        }
        return this.evaluate2(ll, rr);
    }

    evaluate2(_l, _r) {
        throw 'BinaryExpression::evaluate2'
    }
}

class EqExpression extends BooleanBinaryExpr {
    evaluate(l, r, arrowType) {
        // if else
        throw `Unsupported data type in 'eq': ${arrowType}`;
    }
}

function fieldVectorFactory(type, size) {
    throw 'wut?'
}

class ArrowVectorBuilder {
    fieldVector;

    constructor(fieldVector) {
        this.fieldVector = fieldVector;
    }

    set(i, value) {
        throw 'ArrowVectorBuilder::set'
    }

    build() {
        throw 'ArrowVectorBuilder::build'
    }
}

class MathExpression extends BinaryExpression {
    l;
    r;

    constructor(l, r) {
        super();
        this.l = l;
        this.r = r;
    }

    evaluate(l, r) {
        let fieldVector = fieldVectorFactory(l.getType(), l.size());
        let builder = new ArrowVectorBuilder(fieldVector);
        l.forEach((e, i) => {
            let value = this.evaluate3(l.getValue(i), r.getValue(i), l.getType())
            builder.set(i, value);
        })
        return builder.build();
    }

    evaluate3(l, r, type) {
        throw 'MathExpression::evaluate3';
    }
}

class AddExpression extends MathExpression {
    l;
    r;
    type;

    constructor(l, r, type) {
        super();
        this.l = l;
        this.r = r;
        this.type = type;
    }

    evaluate3(_l, _r, type) {
        // if else
        throw `Unsupported data type in AddExpression: ${type}`;
    }

    toString() {
        return `${this.l}+${this.r}`;
    }
}

class AggregateExpression {
    inputExpression() {
        throw 'AggregateExpression::inputExpression';
    }

    /**
     * @return {Accumulator}
     */
    createAccumulator() {
        throw 'AggregateExpression::createAccumulator';
    }
}

class Accumulator {
    accumulate(value) {
        throw 'Accumulator::accumulate';
    }

    finalValue() {
        throw 'Accumulator::finalValue';
    }
}

class MaxAccumulator extends Accumulator {
    value

    accumulate(value) {
        if (value) {
            if (this.value === undefined) {
                this.value = value;
            } else {
                // if else
                throw `MAX is not implemented for data type ${value.getType()}`;
            }
        }
    }

    finalValue() {
        return this.value;
    }
}

class MaxExpression extends AggregateExpression {
    expr;

    constructor(expr) {
        super();
        this.expr = expr;
    }

    inputExpression() {
        return this.expr;
    }

    createAccumulator() {
        return new MaxAccumulator();
    }
}

