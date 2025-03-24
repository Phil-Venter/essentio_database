<?php

namespace Essentio\Database;

use Closure;
use Exception;
use PDO;

/**
 * A simple query builder and executor for interacting with a database using PDO.
 * Provides methods for selecting, inserting, updating, and deleting records.
 */
class Query
{
    /** @var list<string> */
    protected array $columns = ['*'];

    /** @var bool */
    protected bool $nestedFrom = false;

    /** @var string */
    protected string $from = '';

    /** @var list<string> */
    protected array $joins = [];

    /** @var list<string> */
    protected array $unions = [];

    /** @var list<string> */
    protected array $whereClauses = [];

    /** @var list<mixed> */
    protected array $bindings = [];

    /** @var ?int */
    protected ?int $limit = null;

    /** @var ?int */
    protected ?int $offset = null;

    /** @var list<string> */
    protected array $orderBy = [];

    /** @var list<string> */
    protected array $groupBy = [];

    public function __construct(protected PDO $pdo) {}

    /**
     * Specify the columns to select.
     *
     * @param list<string>|string $columns The columns to select.
     * @return static Returns the current query instance.
     */
    public function select(array|string $columns = ['*']): static
    {
        $this->columns = is_array($columns) ? $columns : [$columns];
        return $this;
    }

    /**
     * Set the source for the query.
     *
     * If a Closure is provided, it is executed to build a subquery with an optional alias.
     *
     * @param Closure|string $from The table name or a Closure to build a subquery.
     * @param string|null    $alias The alias for the subquery, if applicable.
     * @return static Returns the current query instance.
     */
    public function from(Closure|string $from, ?string $alias = null): static
    {
        if ($from instanceof Closure) {
            $query = new static($this->pdo);
            $from($query);

            $this->nestedFrom = true;
            $this->from = sprintf('(%s) AS %s', $query->compileSelect(), $alias ?? 't');
            $this->bindings = array_merge($this->bindings, $query->getBindings());

            return $this;
        }

        $this->from = $from;
        return $this;
    }

    /**
     * Set the table to query.
     *
     * This is an alias for the from() method.
     *
     * @param string $table
     * @return static
     */
    public function into(string $table): static
    {
        return $this->from($table);
    }

    /**
     * Set the table to query.
     *
     * This is an alias for the from() method.
     *
     * @param string $table
     * @return static
     */
    public function table(string $table): static
    {
        return $this->from($table);
    }

    /**
     * Add a join clause to the query.
     *
     * @param string      $table The table to join.
     * @param string|null $first The first column for the join condition.
     * @param string|null $operator The operator for the join condition.
     * @param string|null $second The second column for the join condition.
     * @param string      $type The type of join (e.g., INNER, LEFT, RIGHT).
     * @return static Returns the current query instance.
     */
    public function join(string $table, ?string $first = null, ?string $operator = null, ?string $second = null, string $type = ''): static
    {
        $first ??= sprintf('%s.%s_id', $this->from, $table);
        $operator ??= '=';
        $second ??= sprintf('%s.id', $table);

        $this->joins[] = sprintf('%s JOIN %s ON %s %s %s', $type, $table, $first, $operator, $second);

        return $this;
    }

    /**
     * Add an inner join clause to the query.
     *
     * @param string      $table The table to join.
     * @param string|null $first The first column for the join condition.
     * @param string|null $operator The operator for the join condition.
     * @param string|null $second The second column for the join condition.
     * @return static Returns the current query instance.
     */
    public function innerJoin(string $table, ?string $first = null, ?string $operator = null, ?string $second = null): static
    {
        return $this->join($table, $first, $operator, $second, 'INNER');
    }

    /**
     * Add a left join clause to the query.
     *
     * @param string      $table The table to join.
     * @param string|null $first The first column for the join condition.
     * @param string|null $operator The operator for the join condition.
     * @param string|null $second The second column for the join condition.
     * @return static Returns the current query instance.
     */
    public function leftJoin(string $table, ?string $first = null, ?string $operator = null, ?string $second = null): static
    {
        return $this->join($table, $first, $operator, $second, 'LEFT');
    }

    /**
     * Add a right join clause to the query.
     *
     * @param string      $table The table to join.
     * @param string|null $first The first column for the join condition.
     * @param string|null $operator The operator for the join condition.
     * @param string|null $second The second column for the join condition.
     * @return static Returns the current query instance.
     */
    public function rightJoin(string $table, ?string $first = null, ?string $operator = null, ?string $second = null): static
    {
        return $this->join($table, $first, $operator, $second, 'RIGHT');
    }

    /**
     * Add a union clause to the query.
     *
     * Executes a Closure to build a union subquery, and merges its SQL and bindings.
     *
     * @param Closure $callback The callback to build the union query.
     * @param string  $type The type of union (e.g., ALL).
     * @return static Returns the current query instance.
     */
    public function union(Closure $callback, string $type = ''): static
    {
        $query = new static($this->pdo);
        $callback($query);

        $this->unions[] = sprintf(' UNION %s %s', $type, $query->compileSelect());
        $this->bindings = array_merge($this->bindings, $query->getBindings());

        return $this;
    }

    /**
     * Add a UNION ALL clause to the query.
     *
     * Executes a Closure to build the union subquery and merges its SQL and bindings.
     *
     * @param Closure $callback The callback to build the union query.
     * @return static Returns the current query instance.
     */
    public function unionAll(Closure $callback): static
    {
        return $this->union($callback, 'ALL');
    }

    /**
     * Add a where clause to the query.
     *
     * Supports simple conditions, nested conditions via closures, subqueries, and array values.
     *
     * @param string|Closure $column The column name or a Closure for nested conditions.
     * @param string|null    $operator The operator for the condition.
     * @param mixed|null     $value The value for the condition, a Closure for subqueries, or an array for list.
     * @param string         $boolean The boolean operator to chain conditions (AND/OR).
     * @return static Returns the current query instance.
     */
    public function where(string|Closure $column, ?string $operator = null, mixed $value = null, string $boolean = 'AND'): static
    {
        if ($column instanceof Closure) {
            $query = new static($this->pdo);
            $column($query);

            $this->whereClauses[] = [$boolean, '(' . $query->compileWhere() . ')'];
            $this->bindings = array_merge($this->bindings, $query->getBindings());

            return $this;
        }

        if ($value instanceof Closure) {
            $subquery = new static($this->pdo);
            $value($subquery);
            $operator ??= 'IN';

            $this->whereClauses[] = [$boolean, "$column $operator (" . $subquery->compileSelect() . ")"];
            $this->bindings = array_merge($this->bindings, $subquery->getBindings());

            return $this;
        }

        if (is_array($value)) {
            $placeholders = implode(', ', array_fill(0, count($value), '?'));
            $operator ??= 'IN';

            $this->whereClauses[] = [$boolean, "$column $operator ($placeholders)"];
            $this->bindings = array_merge($this->bindings, array_values($value));

            return $this;
        }

        if ($value === null) {
            if ($operator === null) {
                $operator = 'IS';
            } else {
                $value = $operator;
                $operator = '=';
            }
        }

        $this->whereClauses[] = [$boolean, "$column $operator ?"];
        $this->bindings[] = $value;

        return $this;
    }

    /**
     * Add an OR where clause to the query.
     *
     * This is a convenience method that adds a where clause with 'OR' as the boolean operator.
     *
     * @param string|Closure $column The column name or a Closure for nested conditions.
     * @param string|null    $operator The operator for the condition.
     * @param mixed|null     $value The value for the condition.
     * @return static Returns the current query instance.
     */
    public function orWhere(string|Closure $column, ?string $operator = null, mixed $value = null): static
    {
        return $this->where($column, $operator, $value, 'OR');
    }

    /**
     * Add a GROUP BY clause to the query.
     *
     * @param array|string $columns The column or columns to group by.
     * @return static Returns the current query instance.
     */
    public function groupBy(array|string $columns): static
    {
        if (is_array($columns)) {
            $this->groupBy = array_merge($this->groupBy, $columns);
        } else {
            $this->groupBy[] = $columns;
        }

        return $this;
    }

    /**
     * Add an ORDER BY clause to the query.
     *
     * @param string $column The column to order by.
     * @param string $direction The direction of sorting (ASC or DESC).
     * @return static Returns the current query instance.
     */
    public function orderBy(string $column, string $direction = 'ASC'): static
    {
        $this->orderBy[] = sprintf('%s %s', $column, $direction);

        return $this;
    }

    /**
     * Set the LIMIT and OFFSET for the query.
     *
     * @param int $limit The maximum number of records to return.
     * @param int $offset The number of records to skip.
     * @return static Returns the current query instance.
     */
    public function limit(int $limit, int $offset = 0): static
    {
        $this->limit  = $limit;
        $this->offset = $offset;

        return $this;
    }

    /**
     * Execute the query and transform the result using a callback.
     *
     * Fetches all results using PDO::FETCH_FUNC and applies the provided closure to each row.
     *
     * @param Closure $closure The transformation callback.
     * @return mixed Returns the transformed result.
     */
    public function morph(Closure $closure): mixed
    {
        $stmt = $this->pdo?->prepare($this->compileSelect()) ?? throw new Exception;

        $stmt->execute($this->bindings);

        return $stmt->fetchAll(PDO::FETCH_FUNC, $closure);
    }

    /**
     * Execute the query and return all results.
     *
     * @return array Returns the result set as an associative array.
     */
    public function get(): array
    {
        $stmt = $this->pdo?->prepare($this->compileSelect()) ?? throw new Exception;

        $stmt->execute($this->bindings);
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        return $result;
    }

    /**
     * Execute the query and return the first result.
     *
     * @return array|null Returns the first record as an associative array, or null if not found.
     */
    public function first(): ?array
    {
        $this->limit = 1;
        return $this->get()[0] ?? null;
    }

    /**
     * Insert a new record into the table.
     *
     * @param array $data An associative array of column-value pairs.
     * @return int|null Returns the last insert ID on success, or null on failure.
     */
    public function insert(array $data): ?int
    {
        if ($this->nestedFrom && empty($this->from)) {
            throw new Exception('Table not set');
        }

        $columns = implode(', ', array_keys($data));
        $placeholders = implode(', ', array_fill(0, count($data), '?'));
        $sql = sprintf('INSERT INTO %s (%s) VALUES (%s)', $this->from, $columns, $placeholders);

        $stmt = $this->pdo?->prepare($sql) ?? throw new Exception;
        $stmt->execute(array_values($data));
        return $this->pdo->lastInsertId() ? (int) $this->pdo->lastInsertId() : null;
    }

    /**
     * Update records in the table.
     *
     * @param array $data An associative array of column-value pairs to update.
     * @return int Returns the number of rows affected.
     */
    public function update(array $data): int
    {
        if ($this->nestedFrom && empty($this->from)) {
            throw new Exception('Table not set');
        }

        $setParts = [];
        $updateBindings = [];

        foreach ($data as $column => $value) {
            $setParts[] = "$column = ?";
            $updateBindings[] = $value;
        }

        $sql = sprintf('UPDATE %s SET %s', $this->from, implode(', ', $setParts));
        if ($where = $this->compileWhere()) {
            $sql .= sprintf(' WHERE %s', $where);
        }

        $bindings = array_merge($updateBindings, $this->bindings);

        $stmt = $this->pdo?->prepare($sql) ?? throw new Exception;
        $stmt->execute($bindings);
        return $stmt->rowCount();
    }

    /**
     * Delete records from the table.
     *
     * @return int Returns the number of rows deleted.
     */
    public function delete(): int
    {
        if ($this->nestedFrom && empty($this->from)) {
            throw new Exception('Table not set');
        }

        $sql = sprintf('DELETE FROM %s', $this->from);
        if ($where = $this->compileWhere()) {
            $sql .= sprintf(' WHERE %s', $where);
        }

        $stmt = $this->pdo?->prepare($sql) ?? throw new Exception;
        $stmt->execute($this->bindings);
        return $stmt->rowCount();
    }

    /**
     * Compile the SELECT SQL statement.
     *
     * @return string Returns the compiled SQL SELECT statement.
     * @internal
     */
    public function compileSelect(): string
    {
        if (empty($this->from)) {
            throw new Exception('Table not set');
        }

        $sql = sprintf('SELECT %s FROM %s', implode(', ', $this->columns), $this->from);

        if (!empty($this->joins)) {
            $sql .= ' ' . implode(' ', $this->joins);
        }

        if ($where = $this->compileWhere()) {
            $sql .= sprintf(' WHERE %s', $where);
        }

        if (!empty($this->groupBy)) {
            $sql .= sprintf(' GROUP BY %s', implode(', ', $this->groupBy));
        }

        if (!empty($this->orderBy)) {
            $sql .= sprintf(' ORDER BY %s', implode(', ', $this->orderBy));
        }

        if ($this->limit !== null) {
            $sql .= sprintf(' LIMIT %s', $this->limit);
            if ($this->offset !== null && $this->offset > 0) {
                $sql .= sprintf(' OFFSET %s', $this->offset);
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
     * Compile the WHERE clause.
     *
     * @return string Returns the compiled WHERE clause.
     * @internal
     */
    public function compileWhere(): string
    {
        if (empty($this->whereClauses)) {
            return '';
        }

        $sql = '';
        foreach ($this->whereClauses as $index => [$boolean, $condition]) {
            $sql .= $index === 0 ? $condition : sprintf(' %s %s', $boolean, $condition);
        }

        return $sql;
    }

    /**
     * Get the bindings for the prepared statement.
     *
     * @return array Returns an array of bindings.
     * @internal
     */
    public function getBindings(): array
    {
        return $this->bindings;
    }
}
