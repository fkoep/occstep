#[macro_export]
macro_rules! command {
    (
        $vis:vis command $cmd_enum:ident < $state_var:ident : $state_ty:ty > return $ret_enum:ident {
            $(
                on $variant:ident 
                $( ( $( $tuple_field:ident : $tuple_ty:ty ),* $(,)? ) )?
                $( { $( $struct_field:ident : $struct_ty:ty ),* $(,)? } )?
                $( return ( $return_ty:ty ) )?
                do $body:block
            )*
        }
    ) => {
        // The main enum containing all commands
        #[allow(dead_code)]
        $vis enum $cmd_enum {
            $(
                $variant
                $( ( $( $tuple_ty ),* ) )?
                $( { $( $struct_field : $struct_ty ),* } )?
            ),*
        }
 
        // Define the command struct
        $(
        #[allow(dead_code)]
        $vis struct $variant 
            $( ( $( $vis $tuple_ty ),* ) ; )? 
            $( { $( $vis $struct_field : $struct_ty ),* } )?
        )*

        // CommandReturn enum to hold return types
        #[allow(dead_code, unused_parens)]
        $vis enum $ret_enum {
            $(
                $variant( ($( $return_ty  )?) )
            ),*
        }

        // Apply method for executing commands on state
        impl $cmd_enum {
            #[allow(unused)]
            $vis fn apply(self, $state_var: &mut $state_ty) -> $ret_enum {
                match self {
                    $(
                        Self::$variant
                        $( ( $( $tuple_field ),* ) )? 
                        $( { $( $struct_field ),* } )? => {
                            // Perform the action and return a result
                            $ret_enum::$variant((|| $body)())
                        }
                    ),*
                }
            }
        }
    };
}