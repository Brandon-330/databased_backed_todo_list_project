require 'pg'

class DatabasePersistence
  def initialize(logger)
    @db = PG.connect(dbname: 'todos', user: 'postgres', password: 'postgres')
    @logger = logger
  end

  def query(statement, *params)
    @logger.info "#{statement}: #{params}"
    @db.exec_params(statement, params)
  end

  def find_list(id)
    sql = <<~SQL
      SELECT l.*, COUNT(t.id) AS todos_count, COUNT(NULLIF(t.completed, true)) AS todos_remaining_count
      FROM lists AS l
      LEFT JOIN todos AS t
      ON l.id = t.list_id
      WHERE l.id = $1
      GROUP BY l.id
      ORDER BY l.name;
    SQL

    result = query(sql, id)

    tuple = result.first
    tuple_to_list_hash(tuple)
  end

  def find_todos(list_id)
    sql = 'SELECT * FROM todos WHERE list_id = $1'
    result = query(sql, list_id)

    result.map do |tuple|
      is_completed = tuple['completed'] == 't'
      {id: tuple['id'].to_i, name: tuple['name'], completed: is_completed }
    end
  end

  def all_lists
    sql = <<~SQL
      SELECT l.*, COUNT(t.id) AS todos_count, COUNT(NULLIF(t.completed, true)) AS todos_remaining_count
      FROM lists AS l
      LEFT JOIN todos AS t
      ON l.id = t.list_id
      GROUP BY l.id
      ORDER BY l.name;
    SQL

    result = query(sql)

    result.map do |tuple|
      tuple_to_list_hash(tuple)
    end
  end

  def create_new_list(list_name)
    sql = 'INSERT INTO lists (name) VALUES ($1)'
    query(sql, list_name)
  end

  def delete_list(id)
    sql = 'DELETE FROM todos WHERE list_id = $1'
    query(sql, id)

    sql = 'DELETE FROM lists WHERE id = $1'
    query(sql, id)
  end

  def update_list_name(id, new_name)
    sql = 'UPDATE lists SET name = $2 WHERE id = $1'
    query(sql, id, new_name)
  end

  def create_new_todo(list_id, todo_name)
    sql = 'INSERT INTO todos (list_id, name) VALUES ($1, $2)'
    query(sql, list_id, todo_name)
  end

  def delete_todo_from_list(list_id, todo_id)
    sql = 'DELETE FROM todos WHERE id = $1 AND list_id = $2'
    query(sql, todo_id, list_id)
  end

  def update_todo_status(list_id, todo_id, new_status)
    sql = 'UPDATE todos SET completed = $3 WHERE id = $1 AND list_id = $2'
    query(sql, todo_id, list_id, new_status)
  end

  def mark_all_todos_as_completed(list_id)
    sql = 'UPDATE todos SET completed = true WHERE list_id = $1'
    query(sql, list_id)
  end

  private

  def tuple_to_list_hash(tuple)
    { 
      id: tuple['id'].to_i, 
      name: tuple['name'], 
      todos_count: tuple['todos_count'].to_i,
      todos_remaining_count: tuple['todos_remaining_count'].to_i
    }
  end
end