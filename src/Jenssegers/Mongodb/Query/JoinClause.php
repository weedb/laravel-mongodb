<?php

namespace Jenssegers\Mongodb\Query;

use Closure;
use Illuminate\Support\Str;
use Jenssegers\Mongodb\Eloquent\Model;
use MongoDB\BSON\Regex;

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
        $otherWheres = collect($this->wheres)->where('type','!=', 'Column')->all();

        if ($matchBeforeLookup) {
            $pipeline[0] = ['$match' => $matchBeforeLookup];
        }
        $pipeline[1] = [
            '$lookup' => [
                'from'     => $this->getJoinCollection(),
                'as'       => $this->getJoinCollection(),
                'let'      => $this->compileWheresColumnLet($columnWheres),
                'pipeline' => [
                    0 => ['$match' => $this->compileWheresColumn($columnWheres)],
                ]
            ]
        ];
        if ($this->joins) {
            $pipeline[1]['$lookup']['pipeline'] = array_merge($pipeline[1]['$lookup']['pipeline'], $this->compileJoinsAndWheres($this->wheres));
        } elseif (count($otherWheres)){
            $builder->wheres = $otherWheres;
            $ownLookupWheres = $builder->compileWheres();
            $pipeline[1]['$lookup']['pipeline'] = array_merge($pipeline[1]['$lookup']['pipeline'], [0=>['$match' => $ownLookupWheres]]);
        }
        if ($this->type === 'inner'){//if it's inner join, just remove documents, where lookup return empty array
            $pipeline[2]['$match'] = [$this->getJoinCollection()=>['$ne'=>[]]];
        }
        return $pipeline;
    }

    protected function compileWheresColumn(array $columnWheres)
    {
        $collectNameWithDot = $this->getJoinCollection() . '.';
        $compiled = [];
        foreach ($columnWheres as $i => $where) {
            extract($where);
            $isJoinedCollectInFirst = Str::startsWith($first, $collectNameWithDot);
            $primaryColumn = $isJoinedCollectInFirst ? $second : $first;
            $joinedColumn = $isJoinedCollectInFirst ? $first : $second;

            $primaryColumn = "$$" . 'result_' . Str::after($primaryColumn, '.');
            $joinedColumn = "$" . Str::after($joinedColumn, '.');

            if (!isset($operator) || $operator == '=') {
                $result = ['$eq' => [$joinedColumn, $primaryColumn]];
            } elseif (array_key_exists($operator, $this->conversion)) {
                $result = [$this->conversion[$operator] => [$joinedColumn, $primaryColumn]];
            } else {
                $result = ['$' . $operator => [$joinedColumn, $primaryColumn]];
            }
            // The next item in a "chain" of wheres devices the boolean of the
            // first item. So if we see that there are multiple wheres, we will
            // use the operator of the next where.
            if ($i == 0 && count($columnWheres) > 1 && $where['boolean'] == 'and') {
                $where['boolean'] = $columnWheres[$i + 1]['boolean'];
            }
            // Wrap the where with an $or operator.
            if ($where['boolean'] == 'or') {
                $result = ['$or' => [$result]];
            }

            // If there are multiple wheres, we will wrap it with $and. This is needed
            // to make nested wheres work.
            elseif (count($columnWheres) > 1) {
                $result = ['$and' => [$result]];
            }
            $result = ['$expr' => $result];
            // Merge the compiled where with the others.
            $compiled = array_merge_recursive($compiled, $result);
        }
        return $compiled;
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
        return new static($this->newParentQuery(), $this->type, $this->table);
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

    /**
     * Compile the where array.
     * @return array
     */
    public function compileWheres()
    {
        // The wheres to compile.
        $wheres = $this->wheres ?: [];

        // We will add all compiled wheres to this array.
        $compiled = [];

        foreach ($wheres as $i => &$where) {
            // Make sure the operator is in lowercase.
            if (isset($where['operator'])) {
                $where['operator'] = strtolower($where['operator']);

                // Operator conversions
                $convert = [
                    'regexp'        => 'regex',
                    'elemmatch'     => 'elemMatch',
                    'geointersects' => 'geoIntersects',
                    'geowithin'     => 'geoWithin',
                    'nearsphere'    => 'nearSphere',
                    'maxdistance'   => 'maxDistance',
                    'centersphere'  => 'centerSphere',
                    'uniquedocs'    => 'uniqueDocs',
                ];

                if (array_key_exists($where['operator'], $convert)) {
                    $where['operator'] = $convert[$where['operator']];
                }
            }

            // Convert id's.
            if (isset($where['column']) && ($where['column'] == '_id' || Str::endsWith($where['column'], '._id'))) {
                // Multiple values.
                if (isset($where['values'])) {
                    foreach ($where['values'] as &$value) {
                        $value = $this->convertKey($value);
                    }
                } // Single value.
                elseif (isset($where['value'])) {
                    $where['value'] = $this->convertKey($where['value']);
                }
            }

            // Convert DateTime values to UTCDateTime.
            if (isset($where['value'])) {
                if (is_array($where['value'])) {
                    array_walk_recursive($where['value'], function (&$item, $key) {
                        if ($item instanceof DateTime) {
                            $item = new UTCDateTime($item->format('Uv'));
                        }
                    });
                } else {
                    if ($where['value'] instanceof DateTime) {
                        $where['value'] = new UTCDateTime($where['value']->format('Uv'));
                    }
                }
            } elseif (isset($where['values'])) {
                array_walk_recursive($where['values'], function (&$item, $key) {
                    if ($item instanceof DateTime) {
                        $item = new UTCDateTime($item->format('Uv'));
                    }
                });
            }

            // The next item in a "chain" of wheres devices the boolean of the
            // first item. So if we see that there are multiple wheres, we will
            // use the operator of the next where.
            if ($i == 0 && count($wheres) > 1 && $where['boolean'] == 'and') {
                $where['boolean'] = $wheres[$i + 1]['boolean'];
            }

            // We use different methods to compile different wheres.
            $method = "compileWhere{$where['type']}";
            $result = $this->{$method}($where);

            // Wrap the where with an $or operator.
            if ($where['boolean'] == 'or') {
                $result = ['$or' => [$result]];
            }

            // If there are multiple wheres, we will wrap it with $and. This is needed
            // to make nested wheres work.
            elseif (count($wheres) > 1) {
                $result = ['$and' => [$result]];
            }
            $result = ['$expr' => $result];
            // Merge the compiled where with the others.
            $compiled = array_merge_recursive($compiled, $result);
        }

        return $compiled;
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereAll(array $where)
    {
        extract($where);

        return ['$all' => ['$'.$column, array_values($values)]];
    }
    /**
     * @param array $where
     * @return mixed
     */
    protected function compileWhereNested(array $where)
    {
        extract($where);

        $new = $this->newQuery();
        $new->wheres = $query->wheres;
        return $new->compileWheres();
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereBasic(array $where)
    {
        extract($where);

        // Replace like or not like with a Regex instance.
        if (in_array($operator, ['like', 'not like'])) {
            if ($operator === 'not like') {
                $operator = 'not';
            } else {
                $operator = '=';
            }

            // Convert to regular expression.
            $regex = preg_replace('#(^|[^\\\])%#', '$1.*', preg_quote($value));

            // Convert like to regular expression.
            if (!Str::startsWith($value, '%')) {
                $regex = '^' . $regex;
            }
            if (!Str::endsWith($value, '%')) {
                $regex .= '$';
            }

            $value = new Regex($regex, 'i');
        } // Manipulate regexp operations.
        elseif (in_array($operator, ['regexp', 'not regexp', 'regex', 'not regex'])) {
            // Automatically convert regular expression strings to Regex objects.
            if (!$value instanceof Regex) {
                $e = explode('/', $value);
                $flag = end($e);
                $regstr = substr($value, 1, -(strlen($flag) + 1));
                $value = new Regex($regstr, $flag);
            }

            // For inverse regexp operations, we can just use the $not operator
            // and pass it a Regex instence.
            if (Str::startsWith($operator, 'not')) {
                $operator = 'not';
            }
        }

        if (!isset($operator) || $operator == '=') {
            $query = ['$eq'=>['$'.$column, $value]];
        } elseif (array_key_exists($operator, $this->conversion)) {
            $query = [$this->conversion[$operator] => ['$'.$column, $value]];
        } else {
            $query = ['$' . $operator => ['$'.$column, $value]];
        }

        return $query;
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereIn(array $where)
    {
        extract($where);

        return ['$in' => ['$'.$column, array_values($values)]];
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereNotIn(array $where)
    {
        extract($where);

        return ['$nin' => ['$'.$column, array_values($values)]];
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereBetween(array $where)
    {
        extract($where);

        if ($not) {
            return [
                '$or' => [
                    [
                        '$lte' => [$column,$values[0]],
                    ],
                    [
                        '$gte' => [$column,$values[1]],
                    ],
                ],
            ];
        }

        return [
            '$gte' => [$column,$values[0]],
            '$lte' => [$column,$values[1]],
        ];
    }
}
