import BasicTokenizer "BasicTokenizer";

import T "../Types";

module Tokenizer {

    public type Token = T.Token;

    public type Tokenizer = T.Tokenizer;

    public func tokenize(tokenizer_type : Tokenizer, raw_text : Text) : [Token] {
        switch (tokenizer_type) {
            case (#basic) BasicTokenizer.tokenize(raw_text);
        };
    };

};
