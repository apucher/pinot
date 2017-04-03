package com.linkedin.thirdeye.rootcause;

public interface Rewriter {
  String getName();
  Context rewrite(Context context);
}
