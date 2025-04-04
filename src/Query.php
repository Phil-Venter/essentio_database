<?php

namespace Essentio\Database;

use Closure;
use PDO;
use PDOStatement;

class Query
{
    protected array $columns = [];
    protected bool $subquery = false;
    protected string $from = "";
    protected array $joins = [];
    protected array $unions = [];
    protected array $wheres = [];
    protected array $group = [];
    protected array $havings = [];
    protected array $orderBy = [];
    protected ?int $limit = null;
    protected ?int $offset = null;

    protected array $whereBindings = [];
    protected array $havingBindings = [];
    protected array $unionBindings = [];

    public function __construct(protected PDO $pdo) {}

    /**
     * Specifies columns to select.
     *
     * @param string ...$columns Column names.
     * @return static
     */
    public function select(string ...$columns): static
    {
        $this->columns = array_merge($this->columns, $columns);
        return $this;
    }

    /**
     * Specifies the table to select from.
     * Can accept a closure to build a subquery.
     *
     * @param Closure|string $table Table name or closure for subquery.
     * @return static
     */
    public function from(Closure|string $table): static
    {
        assert(empty($this->from));

        if ($table instanceof Closure) {
            return $this->subquery($table);
        }

        $this->from = $table;
        return $this;
    }

    /**
     * Defines a subquery as the source table.
     *
     * @param Closure $table Callback that builds the subquery.
     * @param string|null $as Optional alias.
     * @return static
     */
    public function subquery(Closure $table, ?string $as = null): static
    {
        assert(empty($this->from));

        $query = new static($this->pdo);
        $table($query);

        $this->subquery = true;
        $this->from = sprintf("(%s) AS %s", $query->compileSelect(), $as ?? "t");
        $this->whereBindings = array_merge($this->whereBindings, $query->getBindings());

        return $this;
    }

    /**
     * Adds a JOIN clause to the query.
     *
     * @param string $table Join table.
     * @param string|null $first Left column or 'using'.
     * @param string|null $op Operator or column (if using 'using').
     * @param string|null $second Right column (optional).
     * @param string $type Type of join (INNER, LEFT, etc.).
     * @return static
     */
    public function join(
        string $table,
        ?string $first = null,
        ?string $op = null,
        ?string $second = null,
        string $type = ""
    ): static {
        if (in_array(strtolower($type), ["cross", "natural"])) {
            $this->joins[] = sprintf("%s JOIN %s", $type, $table);
            return $this;
        }

        if ($op !== null && strtolower($first ?? "") === "using") {
            $this->joins[] = sprintf("%s JOIN %s USING(%s)", $type, $table, $op);
            return $this;
        }

        if ($op !== null && $second === null) {
            $second = $op;
            $op = null;
        }

        if ($first === null || $second === null) {
            [$mainTable, $mainAlias] = $this->extractAlias($this->from);
            [$joinTable, $joinAlias] = $this->extractAlias($table);

            $first ??= sprintf("%s.id", $mainAlias ?? $mainTable);
            $second ??= sprintf("%s.%s_id", $joinAlias ?? $joinTable, $mainTable);
        }

        $op ??= "=";

        $this->joins[] = sprintf("%s JOIN %s ON %s %s %s", $type, $table, $first, $op, $second);
        return $this;
    }

    /**
     * Adds a UNION or UNION ALL clause to the query.
     *
     * @param Closure $callback Callback that builds the union subquery.
     * @param string $type Additional UNION keyword like ALL.
     * @return static
     */
    public function union(Closure $callback, string $type = ""): static
    {
        $query = new static($this->pdo);
        $callback($query);

        $this->unions[] = sprintf("UNION %s %s", $type, $query->compileSelect());
        $this->unionBindings = array_merge($this->unionBindings, $query->getBindings());

        return $this;
    }

    /**
     * Adds a WHERE condition to the query.
     *
     * @param string|Closure $column Column name or closure for grouped conditions.
     * @param string|null $op Operator (optional).
     * @param mixed $value Value to compare against (optional).
     * @param string $type Logical operator ("AND" or "OR").
     * @return static
     */
    public function where(string|Closure $column, ?string $op = null, mixed $value = null, string $type = "AND"): static
    {
        [$sql, $bindings] = $this->makeCondition($column, $op, $value, $type, "where");
        $this->wheres[] = $sql;
        $this->whereBindings = array_merge($this->whereBindings, $bindings);
        return $this;
    }

    /**
     * Adds an OR WHERE condition.
     *
     * @param string|Closure $column Column name or closure.
     * @param string|null $op Operator (optional).
     * @param mixed $value Value (optional).
     * @return static
     */
    public function orWhere(string|Closure $column, ?string $op = null, mixed $value = null): static
    {
        return $this->where($column, $op, $value, "OR");
    }

    /**
     * Adds GROUP BY clauses.
     *
     * @param string ...$columns Column names.
     * @return static
     */
    public function group(string ...$columns): static
    {
        $this->group = array_merge($this->group, $columns);
        return $this;
    }

    /**
     * Adds a HAVING condition.
     *
     * @param string|Closure $column Column name or closure for grouped conditions.
     * @param string|null $op Operator.
     * @param mixed $value Value.
     * @param string $type Logical operator.
     * @return static
     */
    public function having(
        string|Closure $column,
        ?string $op = null,
        mixed $value = null,
        string $type = "AND"
    ): static {
        [$sql, $bindings] = $this->makeCondition($column, $op, $value, $type, "having");
        $this->havings[] = $sql;
        $this->havingBindings = array_merge($this->havingBindings, $bindings);
        return $this;
    }

    /**
     * Adds an OR HAVING condition.
     *
     * @param string|Closure $column Column name or closure.
     * @param string|null $op Operator.
     * @param mixed $value Value.
     * @return static
     */
    public function orHaving(string|Closure $column, ?string $op = null, mixed $value = null): static
    {
        return $this->having($column, $op, $value, "OR");
    }

    /**
     * Adds ORDER BY clause.
     *
     * @param string $column Column name.
     * @param string $direction Sort direction ("ASC" or "DESC").
     * @return static
     */
    public function order(string $column, string $direction = "ASC"): static
    {
        $this->orderBy[] = sprintf("%s %s", $column, $direction);
        return $this;
    }

    /**
     * Adds LIMIT and OFFSET clauses.
     *
     * @param int $limit Max number of rows.
     * @param int|null $offset Number of rows to skip.
     * @return static
     */
    public function limit(int $limit, ?int $offset = null): static
    {
        $this->limit = $limit;
        $this->offset = $offset;

        return $this;
    }

    /**
     * Executes a SELECT query and returns all rows.
     *
     * @return array An array of associative rows.
     */
    public function get(): array
    {
        $stmt = $this->smartBind($this->pdo->prepare($this->compileSelect()), $this->getBindings());
        $stmt->execute();
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        return $result;
    }

    /**
     * Lazily yields transformed rows from the SELECT result.
     *
     * @param callable $fn Transformation function, called for each row.
     *                     If callable expects multiple arguments, they are passed from the row by key.
     *                     Ensure keys in SELECT match parameter names.
     * @return iterable<mixed> Generator yielding transformed results.
     */
    public function morph(callable $fn, bool $spread = false): iterable
    {
        $stmt = $this->pdo->prepare($this->compileSelect());
        $stmt = $this->smartBind($stmt, $this->getBindings());
        $stmt->execute();
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            yield $spread ? $fn(...$row) : $fn($row);
        }
    }

    /**
     * Executes a SELECT query and returns the first row.
     *
     * @return array|null Single result row or null.
     */
    public function first(): ?array
    {
        $this->limit = 1;
        return $this->get()[0] ?? null;
    }

    /**
     * Executes the query, returns the first row after applying a transformation.
     *
     * @param callable $fn Function to transform the row.
     *                     If $spread is true, keys are spread as arguments.
     *                     If false, the row is passed as an array.
     * @param bool $spread Whether to spread row keys into arguments.
     * @return mixed|null Transformed result or null if no row.
     */
    public function morphFirst(callable $fn, bool $spread = false): mixed
    {
        $this->limit = 1;
        $stmt = $this->pdo->prepare($this->compileSelect());
        $stmt = $this->smartBind($stmt, $this->getBindings());
        $stmt->execute();
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        if (empty($row)) {
            return null;
        }
        return $spread ? $fn(...$row) : $fn($row);
    }

    /**
     * Executes an INSERT query.
     *
     * @param array $data Key-value pairs of column => value.
     * @return int|null Last inserted ID or null if none.
     */
    public function insert(array $data): ?int
    {
        assert(!empty($data));
        assert(!$this->subquery);
        assert(!empty($this->from));

        $columns = implode(", ", array_keys($data));
        $placeholders = implode(", ", array_fill(0, count($data), "?"));
        $sql = sprintf("INSERT INTO %s (%s) VALUES (%s)", $this->from, $columns, $placeholders);

        $stmt = $this->smartBind($this->pdo->prepare($sql), $data);
        $stmt->execute();
        return $this->pdo->lastInsertId() ? (int) $this->pdo->lastInsertId() : null;
    }

    /**
     * Executes an UPDATE query.
     *
     * @param array $data Key-value pairs of column => value.
     * @return int Number of affected rows.
     */
    public function update(array $data): int
    {
        assert(!empty($data));
        assert(!$this->subquery);
        assert(!empty($this->from));

        $setParts = [];
        $updateBindings = [];

        foreach ($data as $column => $value) {
            $setParts[] = "$column = ?";
            $updateBindings[] = $value;
        }

        $sql = sprintf("UPDATE %s SET %s", $this->from, implode(", ", $setParts));
        if ($where = $this->compileWhere()) {
            $sql .= sprintf(" WHERE %s", $where);
        }

        $stmt = $this->smartBind($this->pdo->prepare($sql), array_merge($updateBindings, $this->getBindings()));
        $stmt->execute();
        return $stmt->rowCount();
    }

    /**
     * Executes a DELETE query.
     *
     * @return int Number of affected rows.
     */
    public function delete(): int
    {
        assert(!$this->subquery);
        assert(!empty($this->from));

        $sql = sprintf("DELETE FROM %s", $this->from);
        if ($where = $this->compileWhere()) {
            $sql .= sprintf(" WHERE %s", $where);
        }

        $stmt = $this->smartBind($this->pdo->prepare($sql), $this->getBindings());
        $stmt->execute();
        return $stmt->rowCount();
    }

    /**
     * Compiles the SQL SELECT statement as a string.
     *
     * @return string SQL query.
     * @internal
     */
    public function compileSelect(): string
    {
        assert(!empty($this->from));

        $sql = sprintf("SELECT %s FROM %s", implode(", ", $this->columns ?: ["*"]), $this->from);

        if (!empty($this->joins)) {
            $sql .= " " . implode(" ", $this->joins);
        }

        if ($where = $this->compileWhere()) {
            $sql .= sprintf(" WHERE %s", $where);
        }

        if (!empty($this->group)) {
            $sql .= sprintf(" GROUP BY %s", implode(", ", $this->group));

            if ($having = $this->compileHaving()) {
                $sql .= sprintf(" HAVING %s", $having);
            }
        }

        if (!empty($this->orderBy)) {
            $sql .= sprintf(" ORDER BY %s", implode(", ", $this->orderBy));
        }

        if ($this->limit !== null) {
            $sql .= sprintf(" LIMIT %s", $this->limit);
            if ($this->offset !== null) {
                $sql .= sprintf(" OFFSET %s", $this->offset);
            }
        }

        if (!empty($this->unions)) {
            foreach ($this->unions as $union) {
                $sql .= $union;
            }
        }

        return $sql;
    }

    /**
     * Compiles the WHERE clause.
     *
     * @return string SQL WHERE clause (no "WHERE" keyword).
     * @internal
     */
    public function compileWhere(): string
    {
        if (empty($this->wheres)) {
            return "";
        }

        return $this->stripLeadingBoolean(implode(" ", $this->wheres));
    }

    /**
     * Compiles the HAVING clause.
     *
     * @return string SQL HAVING clause (no "HAVING" keyword).
     * @internal
     */
    public function compileHaving(): string
    {
        if (empty($this->havings)) {
            return "";
        }

        return $this->stripLeadingBoolean(implode(" ", $this->havings));
    }

    /**
     * Returns all bound parameters across where, having, and unions.
     *
     * @return array All query bindings.
     * @internal
     */
    public function getBindings(): array
    {
        return array_merge($this->whereBindings, $this->havingBindings, $this->unionBindings);
    }

    /**
     * Parses a table string and returns base name and alias (if present).
     *
     * @param string $str Table declaration.
     * @return array Array with [table, alias|null].
     */
    protected function extractAlias(string $str): array
    {
        $str = trim($str);
        $parts = explode(" ", $str);

        if (count($parts) === 3 && strtolower($parts[1]) === "as") {
            return [$parts[0], $parts[2]];
        }

        if (count($parts) === 2) {
            return $parts;
        }

        return [$str, null];
    }

    /**
     * Builds a condition string and binding values.
     *
     * @param string|Closure $column Column or nested condition.
     * @param string|null $op Operator.
     * @param mixed $value Value to bind.
     * @param string $type Logical operator.
     * @param string $clause Clause type ("where" or "having").
     * @return array [string SQL, array bindings]
     */
    protected function makeCondition(
        string|Closure $column,
        ?string $op = null,
        mixed $value = null,
        string $type = "AND",
        string $clause = "where"
    ): array {
        if ($column instanceof Closure) {
            $query = new static($this->pdo);
            $column($query);
            $sql = $clause === "where" ? $query->compileWhere() : $query->compileHaving();

            if (empty(trim($sql))) {
                return [sprintf("%s (%s)", $type, "1=1"), []];
            }

            return [sprintf("%s (%s)", $type, $sql), $query->getBindings()];
        }

        if (is_string($op) && str_contains(strtolower($op), "null")) {
            return [sprintf("%s %s %s", $type, $column, $op), []];
        }

        if ($value instanceof Closure) {
            $query = new static($this->pdo);
            $value($query);
            return [
                sprintf("%s %s %s (%s)", $type, $column, $op ?? "IN", $query->compileSelect()),
                $query->getBindings(),
            ];
        }

        if (is_array($value)) {
            $placeholders = implode(", ", array_fill(0, count($value), "?"));
            return [sprintf("%s %s %s (%s)", $type, $column, $op ?? "IN", $placeholders), $value];
        }

        if ($value === null && $op !== null) {
            $value = $op;
            $op = "=";
        }

        return [sprintf("%s %s %s ?", $type, $column, $op ?? "="), [$value]];
    }

    /**
     * Binds values to a PDOStatement with appropriate types.
     *
     * @param PDOStatement $stmt Prepared statement.
     * @param array $bindings Values to bind.
     * @return PDOStatement Bound statement.
     */
    protected function smartBind(PDOStatement $stmt, array $bindings): PDOStatement
    {
        $values = array_values($bindings);

        foreach ($values as $index => &$value) {
            $stmt->bindParam(
                $index + 1,
                $value,
                match (true) {
                    is_int($value) => PDO::PARAM_INT,
                    is_bool($value) => PDO::PARAM_BOOL,
                    is_null($value) => PDO::PARAM_NULL,
                    default => PDO::PARAM_STR,
                }
            );
        }

        return $stmt;
    }

    /**
     * Removes leading AND/OR from condition strings.
     *
     * @param string $clause SQL clause.
     * @return string Cleaned clause.
     */
    protected function stripLeadingBoolean(string $clause): string
    {
        $trimmed = ltrim($clause);
        if (stripos($trimmed, "AND ") === 0) {
            return substr($trimmed, 4); // length of 'AND '
        }

        if (stripos($trimmed, "OR ") === 0) {
            return substr($trimmed, 3); // length of 'OR '
        }

        return $trimmed;
    }
}
