<?php

namespace Jenssegers\Mongodb\Query;

use Closure;
use Illuminate\Support\Str;
use Jenssegers\Mongodb\Eloquent\Model;

class JoinClause extends Builder
{
    /**
     * Keys of parent wheres at the time before first join was added, or Keys of parent wheres, which was added after last join.
     */
    public array $parentWheresKeys;

    /**
     * The type of join being performed.
     *
     * @var string
     */
    public $type;

    /**
     * The table the join clause is joining to.
     *
     * @var string
     */
    public $table;

    /**
     * The connection of the parent query builder.
     *
     * @var \Illuminate\Database\ConnectionInterface
     */
    protected $parentConnection;

    /**
     * The grammar of the parent query builder.
     *
     * @var Grammar
     */
    protected $parentGrammar;

    /**
     * The processor of the parent query builder.
     *
     * @var Processor
     */
    protected $parentProcessor;

    /**
     * The class name of the parent query builder.
     *
     * @var string
     */
    protected $parentClass;

    /**
     * JoinClause constructor.
     * @param Builder $parentQuery
     * @param $type
     * @param $table
     */
    public function __construct(Builder $parentQuery, $type, string $collectionOrModel)
    {
        $this->type = $type;
        $this->table = $collectionOrModel;

        //set keys of current parent wheres
        $this->setParentWheresKeys($parentQuery);

        $this->parentProcessor = $parentQuery->getProcessor();
        $this->parentConnection = $parentQuery->getConnection();
//        $this->collection = $parentQuery->from;
        parent::__construct(
            $this->parentConnection,
            $this->parentProcessor,
        );
    }

    private function setParentWheresKeys(Builder $parentQuery)
    {
        $existingParentWheresKeys = collect($parentQuery->joins)->pluck('parentWheresKeys')->flatten()->all();
        $parentWheresWithoutColumnType = collect($parentQuery->wheres)->where('type', '!=', 'Column')->all();
        $this->parentWheresKeys = array_keys(array_diff_key($parentWheresWithoutColumnType, array_flip($existingParentWheresKeys)));
    }

    public function getJoinCollection(): string
    {
        //check if model class was passed and get table if it is
        if (class_exists($this->table) && ((new $this->table) instanceof Model)) {
            return (new $this->table)->getTable();
        } else {
            return $this->table;
        }
    }

    public function compileJoin($parentWheres)
    {
        $builder = $this->newQuery();
        $builder->wheres = array_intersect_key($parentWheres, array_flip($this->parentWheresKeys));
        $matchBeforeLookup = $builder->compileWheres();

        $columnWheres = collect($this->wheres)->where('type', 'Column')->all();

        if ($matchBeforeLookup) {
            $pipeline[0] = ['$match' => ['$expr' => $matchBeforeLookup]];
        }
        $pipeline[1] = [
            '$lookup' => [
                'from'     => $this->getJoinCollection(),
                'as'       => $this->getJoinCollection(),
                'let'      => $this->compileWheresColumnLet($columnWheres),
                'pipeline' => [
                    0 => ['$match' => ['$expr' => $this->compileWheresColumn($columnWheres)]],
                ]
            ]
        ];
        if ($this->joins) {
            $pipeline[1]['$lookup']['pipeline'] = array_merge($pipeline[1]['$lookup']['pipeline'], $this->compileJoinsAndWheres($this->wheres));
        }
        return $pipeline;
    }

    protected function compileWheresColumn(array $columnWheres)
    {
        $collectNameWithDot = $this->getJoinCollection() . '.';
        $result = [];
        foreach ($columnWheres as $where) {
            extract($where);
            $isJoinedCollectInFirst = Str::startsWith($first, $collectNameWithDot);
            $primaryColumn = $isJoinedCollectInFirst ? $second : $first;
            $joinedColumn = $isJoinedCollectInFirst ? $first : $second;

            $primaryColumn = "$$" . 'result_' . Str::after($primaryColumn, '.');
            $joinedColumn = "$" . Str::after($joinedColumn, '.');

            if (!isset($operator) || $operator == '=') {
                $query = [$joinedColumn, $primaryColumn];
            } elseif (array_key_exists($operator, $this->conversion)) {
                $query = [$this->conversion[$operator] => [$joinedColumn, $primaryColumn]];
            } else {
                $query = ['$' . $operator => [$joinedColumn, $primaryColumn]];
            }
            $result[] = $query;
        }
        return $result;
    }

    /**
     * Берёт название коллекции к которой осуществляются джоины и на его основе составляет массив из колонок используемых в качестве объединяющих с джоинящейся коллекцией
     * @param array $where
     * @return array[]
     */
    protected function compileWheresColumnLet(array $columnWheres): object
    {
        $collectNameWithDot = $this->getJoinCollection() . '.';
        $result = new \StdClass;
        foreach ($columnWheres as $where) {
            extract($where);
            $isJoinedCollectInFirst = Str::startsWith($first, $collectNameWithDot);
            $primaryColumn = $isJoinedCollectInFirst ? $second : $first;


            $primaryColumn1 = 'result_' . Str::after($primaryColumn, '.');
            $primaryColumn2 = "$" . Str::after($primaryColumn, '.');

            $result->$primaryColumn1 = $primaryColumn2;
        }
        return $result;
    }

    /**
     * Add an "on" clause to the join.
     *
     * On clauses can be chained, e.g.
     *
     *  $join->on('contacts.user_id', '=', 'users.id')
     *       ->on('contacts.info_id', '=', 'info.id')
     *
     * will produce the following SQL:
     *
     * on `contacts`.`user_id` = `users`.`id` and `contacts`.`info_id` = `info`.`id`
     *
     * @param \Closure|string $first
     * @param string|null $operator
     * @param \Illuminate\Database\Query\Expression|string|null $second
     * @param string $boolean
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function on($first, $operator = null, $second = null, $boolean = 'and')
    {
        if ($first instanceof Closure) {
            return $this->whereNested($first, $boolean);
        }

        return $this->whereColumn($first, $operator, $second, $boolean);
    }

    /**
     * Add an "or on" clause to the join.
     *
     * @param \Closure|string $first
     * @param string|null $operator
     * @param string|null $second
     * @return JoinClause
     */
    public function orOn($first, $operator = null, $second = null)
    {
        return $this->on($first, $operator, $second, 'or');
    }

    /**
     * Get a new instance of the join clause builder.
     */
    public function newQuery()
    {
        return parent::newQuery();
    }

    /**
     * Create a new query instance for sub-query.
     */
    protected function forSubQuery()
    {
        return $this->newParentQuery()->newQuery();
    }

    /**
     * Create a new parent query instance.
     */
    protected function newParentQuery()
    {
        return parent::newQuery();
    }
}
