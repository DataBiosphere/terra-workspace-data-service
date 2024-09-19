package org.databiosphere.workspacedataservice.expressions;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.misc.ParseCancellationException;

/*
 According to the documentation, when we use `DefaultErrorStrategy` or `BailErrorStrategy`, the
 `ParserRuleContext.exception` field is set for any parse tree node in the resulting parse tree where an error occurred.
 But this does not cause the parser to throw an exception. Instead the default way is to println the errors and return
 the parsing as a success. What we want is a different way to report the errors. This can be achieved by creating our
 own Listener class and replace the default listeners with instance of this class while creating the parser.
 Reference: https://stackoverflow.com/questions/18132078/handling-errors-in-antlr4/18137301#18137301
*/
public class ErrorThrowingListener extends BaseErrorListener {
  @Override
  public void syntaxError(
      org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
      Object offendingSymbol,
      int line,
      int charPositionInLine,
      String msg,
      org.antlr.v4.runtime.RecognitionException e) {
    var errorMsg =
        "Error while parsing expression. Offending symbol is on line %s at position %d. Error: %s"
            .formatted(line, charPositionInLine, msg);
    throw new ExpressionParsingException(errorMsg, new ParseCancellationException(errorMsg, e));
  }
}
