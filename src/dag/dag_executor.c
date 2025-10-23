// src/dag/dag_executor.c

#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include "roole/dag.h"
#include "roole/common.h"
#include <string.h>
#include <unistd.h>

// ============================================================================
// DAG EXECUTION ENGINE
// ============================================================================

// Check if all dependencies of a step are completed
static int step_dependencies_met(const dag_execution_context_t *ctx, const dag_step_t *step) {
    for (size_t i = 0; i < step->dependency_count; i++) {
        uint32_t dep_id = step->dependencies[i];
        
        // Find dependency step index
        int dep_completed = 0;
        for (size_t j = 0; j < ctx->dag->step_count; j++) {
            if (ctx->dag->steps[j].step_id == dep_id) {
                if (ctx->step_completed[j]) {
                    dep_completed = 1;
                }
                break;
            }
        }
        
        if (!dep_completed) {
            return 0;  // Dependency not met
        }
    }
    
    return 1;  // All dependencies met
}

// Execute a single step (stub implementation)
int dag_execute_step(dag_execution_context_t *ctx, const dag_step_t *step) {
    if (!ctx || !step) return RESULT_ERR_INVALID;
    
    LOG_INFO("Executing step %u: %s (function: %s)", 
                   step->step_id, step->name, step->function_name);
    
    // TODO: Implement actual step execution
    // This should:
    // 1. Load the function handler based on step->function_name
    // 2. Pass ctx->input_data (or intermediate results) to the handler
    // 3. Store result in ctx->output_data
    // 4. Handle timeouts (step->timeout_ms)
    // 5. Handle retries (step->max_retries)
    
    // For now, simulate work
    usleep(100000);  // 100ms
    
    // Stub: just copy input to output
    if (ctx->output_data && ctx->output_capacity >= ctx->input_len) {
        memcpy(ctx->output_data, ctx->input_data, ctx->input_len);
        ctx->output_len = ctx->input_len;
    }
    
    LOG_DEBUG("Step %u completed", step->step_id);
    return RESULT_OK;
}

// Execute entire DAG (topological order)
int dag_execute(dag_execution_context_t *ctx) {
    if (!ctx || !ctx->dag) return RESULT_ERR_INVALID;
    
    ctx->status = DAG_EXEC_STATUS_RUNNING;
    ctx->start_time_ms = time_now_ms();
    
    LOG_INFO("Starting DAG execution %lu (DAG %u: %s)", 
                   ctx->exec_id, ctx->dag->dag_id, ctx->dag->name);
    
    // Initialize step completion tracking
    memset(ctx->step_completed, 0, sizeof(ctx->step_completed));
    
    size_t completed_steps = 0;
    const size_t total_steps = ctx->dag->step_count;
    
    // Simple iterative topological execution
    // Keep trying to execute steps until all are complete or we can't make progress
    while (completed_steps < total_steps) {
        int progress_made = 0;
        
        for (size_t i = 0; i < total_steps; i++) {
            if (ctx->step_completed[i]) {
                continue;  // Already completed
            }
            
            const dag_step_t *step = &ctx->dag->steps[i];
            
            // Check if dependencies are met
            if (!step_dependencies_met(ctx, step)) {
                continue;  // Dependencies not ready
            }
            
            // Execute step
            int result = dag_execute_step(ctx, step);
            
            if (result != RESULT_OK) {
                LOG_ERROR("Step %u failed", step->step_id);
                ctx->status = DAG_EXEC_STATUS_FAILED;
                ctx->end_time_ms = time_now_ms();
                return RESULT_ERR_INVALID;
            }
            
            // Mark as completed
            ctx->step_completed[i] = 1;
            completed_steps++;
            progress_made = 1;
        }
        
        if (!progress_made) {
            // No progress made - circular dependency or missing dependency
            LOG_ERROR("DAG execution stuck - possible circular dependency");
            ctx->status = DAG_EXEC_STATUS_FAILED;
            ctx->end_time_ms = time_now_ms();
            return RESULT_ERR_INVALID;
        }
    }
    
    ctx->status = DAG_EXEC_STATUS_COMPLETED;
    ctx->end_time_ms = time_now_ms();
    
    uint64_t duration_ms = ctx->end_time_ms - ctx->start_time_ms;
    LOG_INFO("DAG execution %lu completed in %lu ms", ctx->exec_id, duration_ms);
    
    return RESULT_OK;
}