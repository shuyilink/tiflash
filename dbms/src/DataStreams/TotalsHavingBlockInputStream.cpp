// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <DataStreams/TotalsHavingBlockInputStream.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{


TotalsHavingBlockInputStream::TotalsHavingBlockInputStream(
    const BlockInputStreamPtr & input_,
    const ExpressionActionsPtr & expression_,
    const std::string & filter_column_,
    TotalsMode totals_mode_)
    : expression(expression_)
    , filter_column_name(filter_column_)
    , totals_mode(totals_mode_)
{
    children.push_back(input_);

    /// Initialize current totals with initial state.

    arena = std::make_shared<Arena>();
    Block source_header = children.at(0)->getHeader();

    current_totals.reserve(source_header.columns());
    for (const auto & elem : source_header)
    {
        if (const auto * column = typeid_cast<const ColumnAggregateFunction *>(elem.column.get()))
        {
            /// Create ColumnAggregateFunction with initial aggregate function state.

            IAggregateFunction * function = column->getAggregateFunction().get();
            auto target = ColumnAggregateFunction::create(column->getAggregateFunction(), Arenas(1, arena));
            AggregateDataPtr data = arena->alloc(function->sizeOfData());
            function->create(data);
            target->getData().push_back(data);
            current_totals.emplace_back(std::move(target));
        }
        else
        {
            /// Not an aggregate function state. Just create a column with default value.

            MutableColumnPtr new_column = elem.type->createColumn();
            elem.type->insertDefaultInto(*new_column);
            current_totals.emplace_back(std::move(new_column));
        }
    }
}


static void finalize(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnWithTypeAndName & current = block.getByPosition(i);
        const auto * unfinalized_type = typeid_cast<const DataTypeAggregateFunction *>(current.type.get());

        if (unfinalized_type)
        {
            current.type = unfinalized_type->getReturnType();
            if (current.column)
                current.column = typeid_cast<const ColumnAggregateFunction &>(*current.column).convertToValues();
        }
    }
}


Block TotalsHavingBlockInputStream::getTotals()
{
    if (!totals)
    {
        totals = children.at(0)->getHeader().cloneWithColumns(std::move(current_totals));
        finalize(totals);
    }

    if (totals && expression)
        expression->execute(totals);

    return totals;
}


Block TotalsHavingBlockInputStream::getHeader() const
{
    Block res = children.at(0)->getHeader();
    finalize(res);
    if (expression)
        expression->execute(res);
    return res;
}


Block TotalsHavingBlockInputStream::readImpl()
{
    Block finalized;
    Block block;

    while (true)
    {
        block = children[0]->read();

        if (!block)
            return finalized;

        finalized = block;
        finalize(finalized);

        total_keys += finalized.rows();

        if (filter_column_name.empty())
        {
            addToTotals(block, nullptr);
        }
        else
        {
            /// Compute the expression in HAVING.
            expression->execute(finalized);

            size_t filter_column_pos = finalized.getPositionByName(filter_column_name);
            ColumnPtr filter_column_ptr = finalized.safeGetByPosition(filter_column_pos).column;

            if (ColumnPtr materialized = filter_column_ptr->convertToFullColumnIfConst())
                filter_column_ptr = materialized;

            FilterDescription filter_description(*filter_column_ptr);

            /// Add values to `totals` (if it was not already done).
            if (totals_mode == TotalsMode::BEFORE_HAVING)
                addToTotals(block, nullptr);
            else
                addToTotals(block, filter_description.data);

            /// Filter the block by expression in HAVING.
            size_t columns = finalized.columns();

            for (size_t i = 0; i < columns; ++i)
            {
                ColumnWithTypeAndName & current_column = finalized.safeGetByPosition(i);
                current_column.column = current_column.column->filter(*filter_description.data, -1);
                if (current_column.column->empty())
                {
                    finalized.clear();
                    break;
                }
            }
        }

        if (!finalized)
            continue;

        passed_keys += finalized.rows();
        return finalized;
    }
}


void TotalsHavingBlockInputStream::addToTotals(const Block & block, const IColumn::Filter * filter)
{
    for (size_t i = 0, num_columns = block.columns(); i < num_columns; ++i)
    {
        const ColumnWithTypeAndName & current = block.getByPosition(i);

        if (const auto * column = typeid_cast<const ColumnAggregateFunction *>(current.column.get()))
        {
            auto & target = typeid_cast<ColumnAggregateFunction &>(*current_totals[i]);
            IAggregateFunction * function = target.getAggregateFunction().get();
            AggregateDataPtr data = target.getData()[0];

            /// Accumulate all aggregate states into that value.

            const ColumnAggregateFunction::Container & vec = column->getData();
            size_t size = vec.size();

            if (filter)
            {
                for (size_t j = 0; j < size; ++j)
                    if ((*filter)[j])
                        function->merge(data, vec[j], arena.get());
            }
            else
            {
                for (size_t j = 0; j < size; ++j)
                    function->merge(data, vec[j], arena.get());
            }
        }
    }
}

} // namespace DB
