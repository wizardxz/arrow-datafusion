# install brew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# install java
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk8
echo 'export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home' >> ~/.zshrc

brew install mvn

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
brew install protobuf

# install xcode
xcode-select --install

# compile code gen
cd ..

git clone -b rust-target https://github.com/rrevenantt/antlr4
cd antlr4
git submodule update --init --recursive --remote

cd runtime/Rust
git remote -v

git remote add fork git@github.com:wizardxz/antlr4rust.git

git remote -v

git fetch fork fix_presto_code_gen
git checkout fork/fix_presto_code_gen

cd ../../
mvn -DskipTests package

ls tool/target/antlr4-4.8-2-SNAPSHOT-complete.jar


cd ..
git clone git@github.com:wizardxz/arrow-datafusion.git -b antlr_parser


cd antlr
cp tool/target/antlr4-4.8-2-SNAPSHOT-complete.jar ../arrow-datafusion/datafusion/sql/src/antlr/
cd ../arrow-datafusion/datafusion/sql/src/antlr/
java -jar antlr4-4.8-2-SNAPSHOT-complete.jar -Dlanguage=Rust Presto.g4 -o presto

java -jar antlr4-4.8-2-SNAPSHOT-complete.jar -Dlanguage=Python3 LexBasic.g4 ANTLRv4Lexer.g4 ANTLRv4Parser.g4 -o antlr -visitor

# develop datafusion
cd arrow-datafusion
cd datafusion
cargo build