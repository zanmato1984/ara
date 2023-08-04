#include "src/foo.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

using namespace ara;

arrow::Result<std::vector<const arrow::compute::HashAggregateKernel*>> GetKernels(
    arrow::compute::ExecContext* ctx,
    const std::vector<arrow::compute::Aggregate>& aggregates,
    const std::vector<arrow::TypeHolder>& in_types) {
  if (aggregates.size() != in_types.size()) {
    return arrow::Status::Invalid(aggregates.size(),
                                  " aggregate functions were specified but ",
                                  in_types.size(), " arguments were provided.");
  }

  std::vector<const arrow::compute::HashAggregateKernel*> kernels(in_types.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto function,
                          ctx->func_registry()->GetFunction(aggregates[i].function));
    ARROW_ASSIGN_OR_RAISE(const arrow::compute::Kernel* kernel,
                          function->DispatchExact({in_types[i], arrow::uint32()}));
    kernels[i] = static_cast<const arrow::compute::HashAggregateKernel*>(kernel);
  }
  return kernels;
}

arrow::Result<std::vector<std::unique_ptr<arrow::compute::KernelState>>> InitKernels(
    const std::vector<const arrow::compute::HashAggregateKernel*>& kernels,
    arrow::compute::ExecContext* ctx,
    const std::vector<arrow::compute::Aggregate>& aggregates,
    const std::vector<arrow::TypeHolder>& in_types) {
  std::vector<std::unique_ptr<arrow::compute::KernelState>> states(kernels.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    const arrow::compute::FunctionOptions* options =
        arrow::internal::checked_cast<const arrow::compute::FunctionOptions*>(
            aggregates[i].options.get());

    if (options == nullptr) {
      // use known default options for the named function if possible
      auto maybe_function = ctx->func_registry()->GetFunction(aggregates[i].function);
      if (maybe_function.ok()) {
        options = maybe_function.ValueOrDie()->default_options();
      }
    }

    arrow::compute::KernelContext kernel_ctx{ctx};
    ARROW_ASSIGN_OR_RAISE(
        states[i],
        kernels[i]->init(&kernel_ctx, arrow::compute::KernelInitArgs{kernels[i],
                                                                     {
                                                                         in_types[i],
                                                                         arrow::uint32(),
                                                                     },
                                                                     options}));
  }

  return std::move(states);
}

arrow::Result<arrow::FieldVector> ResolveKernels(
    const std::vector<arrow::compute::Aggregate>& aggregates,
    const std::vector<const arrow::compute::HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<arrow::compute::KernelState>>& states,
    arrow::compute::ExecContext* ctx, const std::vector<arrow::TypeHolder>& types) {
  arrow::FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    arrow::compute::KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    ARROW_ASSIGN_OR_RAISE(auto type, kernels[i]->signature->out_type().Resolve(
                                         &kernel_ctx, {types[i], arrow::uint32()}));
    fields[i] = field(aggregates[i].function, type.GetSharedPtr());
  }
  return fields;
}

TEST(FooTest, Foo) {
  auto ec = ara::Foo();
  arrow::Int8Builder builder(ec.memory_pool());
  auto res = builder.Append(8);
  ASSERT_TRUE(res.ok());
  auto a = builder.Finish();
  ASSERT_TRUE(a.ok());
  std::cout << a->get()->ToString() << std::endl;
}
