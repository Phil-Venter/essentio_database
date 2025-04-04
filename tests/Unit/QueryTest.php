<?php

use Essentio\Database\Query;

beforeEach(function () {
    $this->pdo = new PDO("sqlite::memory:");
    $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $this->pdo->exec("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            status TEXT
        );
    ");

    $this->pdo->exec("
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT
        );
    ");
});

describe(Query::class, function () {
    it("can insert and fetch a row", function () {
        $id = new Query($this->pdo)->from("users")->insert([
            "name" => "Grug",
            "email" => "grug@cave.hut",
            "status" => "active",
        ]);

        expect($id)->toBeInt();

        $user = new Query($this->pdo)->select("id", "name")->from("users")->first();
        expect($user)->toHaveKeys(["id", "name"]);
        expect($user["name"])->toBe("Grug");
    });

    it("can update data", function () {
        $id = new Query($this->pdo)->from("users")->insert([
            "name" => "Grug",
            "email" => "grug@cave.hut",
            "status" => "asleep",
        ]);

        $count = new Query($this->pdo)
            ->from("users")
            ->where("id", $id)
            ->update([
                "status" => "awake",
            ]);

        expect($count)->toBe(1);

        $status = new Query($this->pdo)->select("status")->from("users")->where("id", $id)->first();
        expect($status["status"])->toBe("awake");
    });

    it("can delete data", function () {
        $id = new Query($this->pdo)->from("users")->insert([
            "name" => "Ugluk",
            "email" => "ugluk@orc.net",
            "status" => "frozen",
        ]);

        $deleted = new Query($this->pdo)->from("users")->where("id", $id)->delete();
        expect($deleted)->toBe(1);

        $user = new Query($this->pdo)->from("users")->where("id", $id)->first();
        expect($user)->toBeNull();
    });

    it("can build query with joins and group by", function () {
        $userId = new Query($this->pdo)->from("users")->insert([
            "name" => "Grug",
            "email" => "grug@cave.hut",
            "status" => "active",
        ]);

        new Query($this->pdo)->from('posts')->insert(['user_id' => $userId, 'title' => 'Hello']);
        new Query($this->pdo)->from('posts')->insert(['user_id' => $userId, 'title' => 'Again']);

        $result = (new Query($this->pdo))
            ->select("u.name", "COUNT(p.id) AS count")
            ->from("users u")
            ->join("posts p", second: 'p.user_id')
            ->group("u.id")
            ->having('COUNT(p.id)', '>=', 2)
            ->first();

        expect($result)->not()->toBeNull();
        expect($result["name"])->toBe("Grug");
    });

    it("can use subquery in from()", function () {
        new Query($this->pdo)->from("users")->insert([
            "name" => "Subgrug",
            "email" => "sub@grug.com",
            "status" => "nested",
        ]);

        $result = new Query($this->pdo)
            ->from(function ($q) {
                $q->select("name")->from("users")->where("status", "=", "nested");
            }, "u")
            ->select("name")
            ->first();

        expect($result["name"])->toBe("Subgrug");
    });

    it("can do union query", function () {
        new Query($this->pdo)->from("users")->insert([
            "name" => "One",
            "email" => "1@hut.com",
            "status" => "active",
        ]);

        new Query($this->pdo)->from("users")->insert([
            "name" => "Two",
            "email" => "2@hut.com",
            "status" => "inactive",
        ]);

        $main = (new Query($this->pdo))
            ->select("name")
            ->from("users")
            ->where("status", "=", "active")
            ->union(function ($q) {
                $q->select("name")->from("users")->where("status", "=", "inactive");
            });

        $result = $main->get();
        expect(count($result))->toBe(2);
    });

    it("returns correct bindings for where and having", function () {
        $query = new Query($this->pdo)->select("*")->from("users")->where("status", "=", "alive")->having("COUNT(id)", ">", 1);
        expect($query->getBindings())->toBe(["alive", 1]);
    });

    it("can use where with IN array", function () {
        new Query($this->pdo)->from("users")->insert([
            "name" => "G",
            "email" => "g@hut.com",
            "status" => "alive",
        ]);

        $users = new Query($this->pdo)
            ->select("id")
            ->from("users")
            ->where("status", "IN", ["alive", "asleep"])
            ->get();

        expect(count($users))->toBeGreaterThan(0);
    });

    it("throws when missing table for insert", function () {
        $query = new Query($this->pdo);
        $query->insert(["x" => 1]); // no from()
    })->throws(Exception::class, "Table not set");

    it("throws when missing table for update", function () {
        $query = new Query($this->pdo);
        $query->update(["x" => 1]); // no from()
    })->throws(Exception::class, "Table not set");

    it("throws when missing table for delete", function () {
        $query = new Query($this->pdo);
        $query->delete(); // no from()
    })->throws(Exception::class, "Table not set");

    it("can get only first row", function () {
        new Query($this->pdo)->from("users")->insert([
            "name" => "GrugFirst",
            "email" => "first@grug.com",
            "status" => "test",
        ]);

        $row = new Query($this->pdo)->select("*")->from("users")->first();
        expect($row)->toBeArray();
    });
});
