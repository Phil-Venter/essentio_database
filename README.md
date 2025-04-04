# Essentio\Query

Essentio\Query gives you fluent, precise control over SQL without pulling in layers of abstraction.
There’s no “Model”, no “Repository”, and no assumptions. You write the query. It builds and executes it.

No fluff. No black boxes. Just SQL, made easier.

---

## Basic Usage

```php
use Essentio\Database\Query;

$query = new Query($pdo);
```

---

## API Reference (with examples)

---

### `select(...$columns): static`

Set the columns to select.

```php
$query->select('id', 'name');
```

---

### `from(string|Closure $table): static`

Set the FROM clause, or provide a subquery via closure.

```php
$query->from('users');

// Subquery example:
$query->from(fn($q) => $q->select('id')->from('users')); // defaults to AS t
```

---

### `subquery(Closure $builder, ?string $alias = null): static`

Explicit version of `from()` with closure for subqueries.

```php
$query->subquery(fn($q) => $q->select('id')->from('users'), 'sub');
```

---

### `join(string $table, ?string $first = null, ?string $op = null, ?string $second = null, string $type = ''): static`

Add a JOIN clause. You can be explicit, or let it fill in the blanks using conventions.

```php
$query->join('profiles', 'users.id', '=', 'profiles.user_id', 'LEFT');
```

---

### `union(Closure $builder, string $type = ''): static`

UNION another query into this one.

```php
$query
    ->select('id')->from('users')
    ->union(fn($q) => $q->select('id')->from('archived_users'));
```

---

### `where(string|Closure $column, ?string $op = null, mixed $value = null, string $type = 'AND'): static`

Add a WHERE condition. Nest with closures.

```php
// Basic equality
$query->where('status', '=', 'active'); // WHERE status = ? ; bound: ['active']

// Operator inference (value-only)
$query->where('status', 'active'); // WHERE status = ? ; bound: ['active']

// NULL-safe condition
$query->where('deleted_at', 'IS NULL'); // WHERE deleted_at IS NULL

// Array value → `IN (...)`
$query->where('id', 'IN', [1, 2, 3]); // WHERE id IN (?, ?, ?) ; bound: [1, 2, 3]

// Closure: nested conditions
$query->where(fn(Query $q) =>
    $q->where('type', '=', 'admin')
      ->orWhere('verified', '=', true)
); // WHERE (type = ? OR verified = ?) ; bound ['admin', true]

// Closure as subquery
$query->where('user_id', 'IN', fn($q) =>
    $q->select('id')->from('users')->where('active', true)
); // WHERE user_id IN (SELECT id FROM users WHERE active = ?) ; bound [true]
```

---

### `orWhere(...)`

Same as `where()` but with an OR.

```php
$query->orWhere('deleted_at', 'IS', 'NULL');
```

---

### `group(...$columns): static`

Add a GROUP BY clause.

```php
$query->group('role', 'status');
```

---

### `having(string|Closure $column, ?string $op = null, mixed $value = null, string $type = 'AND'): static`

Add a HAVING condition. Works just like `where()`, but applies after `GROUP BY`.

```php
$query->group('type')->having('COUNT(id)', '>', 10);
```

---

### `orHaving(...)`

Same as `having()` but with OR.

```php
$query->orHaving('SUM(score)', '>', 100);
```

---

### `order(string $column, string $direction = 'ASC'): static`

Add an ORDER BY clause.

```php
$query->order('created_at', 'DESC');
```

---

### `limit(int $limit, ?int $offset = null): static`

Apply a LIMIT (and optional OFFSET).

```php
$query->limit(10);              // First 10
$query->limit(10, 20);          // 10 rows, starting from 20
```

---

### `get(): array`

Execute the built query and return all rows as an array.

```php
$users = $query->select('id', 'email')->from('users')->get();
```

---

### `first(): ?array`

Like `get()` but returns just the first row (or null).

```php
$user = $query->select('*')->from('users')->where('id', '=', 1)->first();
```

---

### `morph(callable $fn, bool $spread = false): iterable`

Transforms each row using a callable. Can spread array values as arguments.

```php
$query->select('name', 'email')->from('users')->morph(fn($row) => strtoupper($row['name']));

$query->morph(fn($name, $email) => "$name <$email>", true);
```

---

### `insert(array $data): ?int`

Insert a row and return the last insert ID (if available).

```php
$id = $query->from('users')->insert([
    'name' => 'Jane',
    'email' => 'jane@example.com'
]);
```

---

### `update(array $data): int`

Update rows matching the query. Returns affected count.

```php
$affected = $query->from('users')->where('id', '=', 5)->update(['name' => 'New Name']);
```

---

### `delete(): int`

Delete rows matching the query. Returns affected count.

```php
$deleted = $query->from('users')->where('banned', '=', true)->delete();
```

---

## Final Notes

- Every method mutates the query. You can chain as much as you like.
- If something isn’t working, read the compiled SQL.
- Assertions exist for your own safety. Respect them.
- It doesn’t catch your mistakes — and it doesn’t try to.

---

## Why This Exists

I needed a query builder that gave me control and stayed out of the way.
This one does. That’s it. It’s stable, fast, and transparent.

If you like it — use it. If not — no hard feelings.
