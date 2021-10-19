/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.Unnest;
import com.google.common.collect.ImmutableList;

import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;

// TODO: Add more cases https://github.com/prestodb/presto/issues/16032
public class MaterializedViewPlanValidator
        extends DefaultTraversalVisitor<Void, MaterializedViewPlanValidator.MaterializedViewPlanValidatorContext>
{
    protected MaterializedViewPlanValidator()
    {}

    public static void validate(Query viewQuery)
    {
        new MaterializedViewPlanValidator().process(viewQuery, new MaterializedViewPlanValidatorContext());
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, MaterializedViewPlanValidatorContext context)
    {
        process(node.getSelect(), context);
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        if (node.getWhere().isPresent()) {
            Expression subquery = node.getWhere().get();
            boolean wasWithinWhere = context.isWithinWhere();

            context.setWithinWhere(true);
            process(subquery, context);
            context.setWithinWhere(wasWithinWhere);
        }
        if (node.getGroupBy().isPresent()) {
            process(node.getGroupBy().get(), context);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        return null;
    }

    @Override
    protected Void visitTable(Table node, MaterializedViewPlanValidatorContext context)
    {
        if (context.isWithinWhere()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Subqueries inside where in materialized view are not supported yet");
        }
        return super.visitTable(node, context);
    }

    @Override
    protected Void visitJoin(Join node, MaterializedViewPlanValidatorContext context)
    {
        context.pushJoinNode(node);

        if (context.getJoinNodes().size() > 1) {
            throw new SemanticException(NOT_SUPPORTED, node, "More than one join in materialized view is not supported yet.");
        }

        switch (node.getType()) {
            case INNER:
                if (!node.getCriteria().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Inner join with no criteria is not supported for materialized view.");
                }

                JoinCriteria joinCriteria = node.getCriteria().get();
                if (!(joinCriteria instanceof JoinOn)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Only join-on is supported for materialized view.");
                }

                process(node.getLeft(), context);
                process(node.getRight(), context);

                context.setProcessingJoinNode(true);
                if (joinCriteria instanceof JoinOn) {
                    process(((JoinOn) joinCriteria).getExpression(), context);
                }
                context.setProcessingJoinNode(false);

                break;
            case CROSS:
                if (!(node.getRight() instanceof AliasedRelation)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Cross join is supported only with unnest for materialized view.");
                }
                AliasedRelation right = (AliasedRelation) node.getRight();
                if (!(right.getRelation() instanceof Unnest)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Cross join is supported only with unnest for materialized view.");
                }

                process(node.getLeft(), context);

                break;

            default:
                throw new SemanticException(NOT_SUPPORTED, node, "Only inner join and cross join unnested are supported for materialized view.");
        }

        context.popJoinNode();
        return null;
    }

    @Override
    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, MaterializedViewPlanValidatorContext context)
    {
        if (context.isProcessingJoinNode()) {
            if (!node.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only AND operator is supported for join criteria for materialized view.");
            }
        }

        return super.visitLogicalBinaryExpression(node, context);
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, MaterializedViewPlanValidatorContext context)
    {
        if (context.isProcessingJoinNode()) {
            if (!node.getOperator().equals(ComparisonExpression.Operator.EQUAL)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only EQUAL join is supported for materialized view.");
            }
        }

        return super.visitComparisonExpression(node, context);
    }

    public static final class MaterializedViewPlanValidatorContext
    {
        private boolean isProcessingJoinNode;
        private boolean isWithinWhere;
        private final LinkedList<Join> joinNodeStack;

        public MaterializedViewPlanValidatorContext()
        {
            isProcessingJoinNode = false;
            joinNodeStack = new LinkedList<>();
            isWithinWhere = false;
        }

        public boolean isProcessingJoinNode()
        {
            return isProcessingJoinNode;
        }

        public boolean isWithinWhere()
        {
            return isWithinWhere;
        }

        public void setProcessingJoinNode(boolean processingJoinNode)
        {
            isProcessingJoinNode = processingJoinNode;
        }

        public void setWithinWhere(boolean withinWhere)
        {
            isWithinWhere = withinWhere;
        }

        public void pushJoinNode(Join join)
        {
            joinNodeStack.push(join);
        }

        public Join popJoinNode()
        {
            return joinNodeStack.pop();
        }

        public Join getTopJoinNode()
        {
            return joinNodeStack.getFirst();
        }

        public List<Join> getJoinNodes()
        {
            return ImmutableList.copyOf(joinNodeStack);
        }
    }
}
